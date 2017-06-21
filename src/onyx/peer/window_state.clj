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
  (segment-triggers! [this state-event])
  (all-triggers! [this state-event])
  (aggregate-state [this state-event])
  (apply-extents [this state-event])
  (apply-event [this state-event]))

(defn rollup-result [segment]
  (cond (sequential? segment) 
        segment 
        (map? segment)
        (list segment)
        :else
        (throw (ex-info "Value returned by :trigger/emit must be either a hash-map or a sequential of hash-maps." 
                        {:value segment}))))

(defrecord WindowExecutor [window-extension grouping-fn triggers window id idx state-store 
                           init-fn emitted create-state-update apply-state-update super-agg-fn event-results]
  StateEventReducer
  (window-id [this]
    (:window/id window))

  (trigger-extent! [this state-event trigger-record extent]
    (let [{:keys [sync-fn emit-fn trigger create-state-update apply-state-update]} trigger-record
          group-id (:group-id state-event)
          extent-state (st/get-extent state-store idx group-id extent)
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
      (st/put-extent! state-store idx group-id extent new-extent-state)))

  (trigger [this state-event trigger-record]
    (let [{:keys [trigger trigger-fire? fire-all-extents?]} trigger-record 
          state-event (-> state-event 
                          (assoc :window window) 
                          (assoc :trigger-state trigger-record))
          group-id (:group-id state-event)
          trigger-idx (:idx trigger-record)
          trigger-state (st/get-trigger state-store trigger-idx group-id)
          ;; FIXME, should check if key not found, not just nil...
          trigger-state (if (nil? trigger-state)
                          ((:init-state trigger-record) trigger)
                          trigger-state)
          next-trigger-state-fn (:next-trigger-state trigger-record)
          new-trigger-state (next-trigger-state-fn trigger trigger-state state-event)
          fire-all? (or fire-all-extents? (not= (:event-type state-event) :segment))
          fire-extents (if fire-all? 
                         (st/group-extents state-store idx group-id)
                         (:extents state-event))]
      ;(println "EXTENTS" fire-extents)
      (st/put-trigger! state-store trigger-idx group-id new-trigger-state)
      (run! (fn [extent] 
              (let [[lower upper] (we/bounds window-extension extent)
                    state-event (-> state-event
                                    (assoc :lower-bound lower)
                                    (assoc :upper-bound upper))]
                (when (trigger-fire? trigger new-trigger-state state-event)
                  (trigger-extent! this state-event trigger-record extent))))
            ;; FIXME, shouldn't be any need to sort here
            ;; FIXME, shouldn't be any need to sort here
            (sort fire-extents))
      this))

  (segment-triggers! [this state-event]
    ;; FIXME, have a trigger idx -> trigger map instead of a trigger states vector
    ;; We can run through it in order anyway
    (run! (fn [[idx trigger-state]] 
            (trigger this state-event trigger-state))
          triggers)
    state-event)

  (all-triggers! [this state-event]
    (println "TRIGGERKEYS" )
    ;; FIXME, have a trigger idx -> trigger map instead of a trigger states vector
    ;; We can run through it in order anyway
    (run! (fn [[trigger-idx group-bytes group-key]] 
            (println "IDX" trigger-idx (keys triggers))
            (trigger this
                     (-> state-event
                         (assoc :group-id group-bytes)
                         (assoc :group-key group-key)) 
                     (get triggers trigger-idx)))
          (st/trigger-keys state-store))
    state-event)

  (aggregate-state [this state-event]
    (let [{:keys [segment group-id extents]} state-event]
      (run! (fn [extent] 
              (let [extent-state (st/get-extent state-store idx group-id extent)
                    extent-state (default-state-value init-fn window extent-state)
                    transition-entry (create-state-update window extent-state segment)
                    new-extent-state (apply-state-update window extent-state transition-entry)]
                (st/put-extent! state-store idx group-id extent new-extent-state)))
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
             (segment-triggers! this)))
      (all-triggers! this state-event))
    this))

(defn fire-state-event [windows-state state-event]
  (mapv (fn [ws]
          (apply-event ws state-event))
        windows-state))

(defn process-segment
  [state state-event]
  (let [{:keys [grouping-fn onyx.core/results] :as event} (get-event state)
        state-store (get-state-store state)
        _ (assert state-store)
        grouped? (not (nil? grouping-fn))
        grouping-fn (or grouping-fn (fn [_] nil))
        state-event* (assoc state-event :grouped? grouped?)
        windows-state (get-windows-state state)
        updated-states (reduce 
                        (fn [windows-state* segment]
                          (if (exception? segment)
                            windows-state*
                            (let [group-key (grouping-fn segment)
                                  state-event** (-> state-event*
                                                    (assoc :segment segment)
                                                    (assoc :group-id (st/group-id state-store group-key))
                                                    (assoc :group-key group-key))]
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
