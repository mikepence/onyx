(ns ^:no-doc onyx.peer.window-state
    (:require [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
              [schema.core :as s]
              [clojure.core.async :refer [alts!! <!! >!! <! >! timeout chan close! thread go]]
              [onyx.schema :refer [TriggerState WindowExtension Window Event]]
              [onyx.state.protocol.db :as st]
              [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
              [onyx.windowing.window-extensions :as we]
              [onyx.protocol.task-state :refer :all]
              [onyx.types :refer [->MonitorEvent new-state-event]]
              [onyx.state.state-extensions :as state-extensions]
              [onyx.static.default-vals :refer [arg-or-default]]
              [onyx.static.util :refer [exception?]]))

(s/defn default-state-value 
  [init-fn window state-value]
  (or state-value (init-fn window)))

(defprotocol StateEventReducer
  (window-id [this])
  (trigger-extent! [this state-event trigger-record extent])
  (trigger [this state-event trigger-record])
  (triggers! [this state-event])
  (aggregate-state [this state-event])
  (apply-extents [this state-event])
  (apply-event [this state-event])
  (recover-state [this dumped])
  (export-state [this]))

(defn rollup-result [segment]
  (cond (sequential? segment) 
        segment 
        (map? segment)
        (list segment)
        :else
        (throw (ex-info "Value returned by :trigger/emit must be either a hash-map or a sequential of hash-maps." 
                        {:value segment}))))

(defrecord WindowExecutor [window-extension grouping-fn trigger-states window id state-store init-fn emitted 
                           create-state-update apply-state-update super-agg-fn event-results]
  StateEventReducer
  (window-id [this]
    (:window/id window))

  (trigger-extent! [this state-event trigger-record extent]
    (let [{:keys [sync-fn emit-fn trigger create-state-update apply-state-update]} trigger-record
          group-key (:group-key state-event)
          extent-state (st/get-extent state-store id group-key extent)
          state-event (-> state-event
                          (assoc :extent extent)
                          (assoc :extent-state extent-state))
          entry (create-state-update trigger extent-state state-event)
          new-extent-state (apply-state-update trigger extent-state entry)
          state-event (-> state-event
                          (assoc :next-state new-extent-state)
                          (assoc :trigger-update entry))
          emit-segment (when emit-fn 
                         (emit-fn (:task-event state-event) 
                                  window trigger state-event extent-state))]
      (when sync-fn 
        (sync-fn (:task-event state-event) window trigger state-event extent-state))
      (when emit-segment 
        (swap! emitted (fn [em] (into em (rollup-result emit-segment)))))
      (st/put-extent! state-store id group-key extent new-extent-state)))

  (trigger [this state-event trigger-record]
    (let [{:keys [trigger trigger-fire? fire-all-extents?]} trigger-record 
          state-event (-> state-event 
                          (assoc :window window) 
                          (assoc :trigger-state trigger-record))
          group-key (:group-key state-event)
          trigger-id (:trigger/id trigger-record)
          trigger-state (st/get-trigger state-store id trigger-id group-key)
          ;_ (println "TRIGGERSTATE" trigger-state)
          ;; FIXME, should check if key not found, not just nil...
          trigger-state (if (nil? trigger-state)
                          ((:init-state trigger-record) trigger)
                          trigger-state)
          next-trigger-state-fn (:next-trigger-state trigger-record)
          new-trigger-state (next-trigger-state-fn trigger trigger-state state-event)
          fire-all? (or fire-all-extents? (not= (:event-type state-event) :segment))
          fire-extents (if fire-all? 
                         (st/group-extents state-store id group-key)
                         (:extents state-event))]
      ;(println "EXTENTS" fire-extents)
      (st/put-trigger! state-store id trigger-id group-key new-trigger-state)
      (run! (fn [extent] 
              (let [bounds (we/bounds window-extension extent)
                    state-event (-> state-event
                                    (assoc :lower-bound (first bounds))
                                    (assoc :upper-bound (second bounds)))]
                (when (trigger-fire? trigger new-trigger-state state-event)
                  (trigger-extent! this state-event trigger-record extent))))
            fire-extents)
      this))

  (export-state [this]
    (st/export state-store id))

  (recover-state [this bs]
    (st/restore! state-store id bs)
    this)

  (triggers! [this state-event]
    (run! (fn [trigger-state] 
            (trigger this state-event trigger-state))
          trigger-states)
    state-event)

  (aggregate-state [this state-event]
    (let [{:keys [segment group-key extents]} state-event]
      (run! (fn [extent] 
              (let [extent-state (st/get-extent state-store id group-key extent)
                    extent-state (default-state-value init-fn window extent-state)
                    transition-entry (create-state-update window extent-state segment)
                    new-extent-state (apply-state-update window extent-state transition-entry)]
                (st/put-extent! state-store id group-key extent new-extent-state)))
            extents))
    state-event)

  (apply-extents [this state-event]
    (let [segment-coerced (we/uniform-units window-extension (:segment state-event))
          ;; FIXME, needed for windowed changes
          ;new-state (swap! state-store #(we/speculate-update window-extension % segment-coerced))
          extents (we/extents window-extension 
                              nil
                              #_(keys new-state) 
                              segment-coerced)]
      (-> state-event
          (assoc :extents extents)
          (assoc :segment-coerced segment-coerced))))

  (apply-event [this state-event]
    (if (= (:event-type state-event) :new-segment)
      (let [merge-extents-fn 
            ; (fn [{:keys [state-store] :as t} 
            ;      {:keys [segment-coerced] :as state-event}]
            ;   (swap! state-store #(we/merge-extents window-extension % super-agg-fn segment-coerced))
            ;   state-event)
            ;; merge-extents is currently broken
            (fn [this state-event]
              state-event)] 
        (->> state-event
             (apply-extents this)
             (aggregate-state this)
             (merge-extents-fn this)
             (triggers! this)))
      (triggers! this state-event))
    this))

(defn fire-state-event [windows-state state-event]
  (mapv (fn [ws]
          (apply-event ws state-event))
        windows-state))

(defn process-segment
  [state state-event]
  (let [{:keys [grouping-fn onyx.core/results] :as event} (get-event state)
        grouped? (not (nil? grouping-fn))
        state-event* (assoc state-event :grouped? grouped?)
        windows-state (get-windows-state state)
        updated-states (reduce 
                        (fn [windows-state* segment]
                          (if (exception? segment)
                            windows-state*
                            (let [state-event** (cond-> (assoc state-event* :segment segment)
                                                  grouped? (assoc :group-key (grouping-fn segment)))]
                              (fire-state-event windows-state* state-event**))))
                        windows-state
                        (mapcat :leaves (:tree results)))
        emitted (doall (mapcat (comp deref :emitted) updated-states))]
    (run! (fn [w] (reset! (:emitted w) [])) windows-state)
    (-> state 
        (set-windows-state! updated-states)
        (update-event! (fn [e] (update e :onyx.core/triggered into emitted))))))

(defn process-event [state state-event]
  (set-windows-state! state (fire-state-event (get-windows-state state) state-event)))

(defn assign-windows [state event-type]
  (let [state-event (new-state-event event-type (get-event state))] 
    (if (= :new-segment event-type)
      (process-segment state state-event)
      (process-event state state-event))))
