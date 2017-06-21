(ns onyx.peer.resume-point
  (:require [onyx.compression.nippy :refer [checkpoint-compress checkpoint-decompress]]
            [onyx.extensions :as extensions]
            [onyx.peer.window-state :as ws]
            [onyx.checkpoint :as checkpoint]
            [onyx.state.memory]
            [onyx.state.lmdb]
            [taoensso.timbre :refer [debug info error warn trace fatal]]
            [onyx.windowing.window-compile :as wc]))

(defn coordinates->input-resume-point
  [{:keys [onyx.core/task-id onyx.core/job-id onyx.core/resume-point onyx.core/tenancy-id] :as event}
   latest-coordinates]
  (if latest-coordinates
    (merge latest-coordinates
           {:tenancy-id tenancy-id
            :job-id job-id
            :task-id task-id
            :slot-migration :direct})
    (:input resume-point)))

(defn coordinates->output-resume-point
  [{:keys [onyx.core/task-id onyx.core/job-id onyx.core/resume-point onyx.core/tenancy-id] :as event}
   latest-coordinates]
  (if latest-coordinates
    (merge latest-coordinates
           {:tenancy-id tenancy-id
            :job-id job-id
            :task-id task-id
            :slot-migration :direct})
    (:output resume-point)))

(defn coordinates->windows-resume-point
  [{:keys [onyx.core/windows onyx.core/task-id
           onyx.core/job-id onyx.core/resume-point
           onyx.core/tenancy-id] :as event}
   latest-coordinates]
  (if latest-coordinates
    (reduce (fn [m {:keys [window/id]}]
              (assoc m id (merge latest-coordinates
                                 {:mode :resume
                                  :tenancy-id tenancy-id
                                  :job-id job-id
                                  :task-id task-id
                                  :slot-migration :direct})))
            {}
            windows)
    (:windows resume-point)))

(defn read-checkpoint
  [{:keys [onyx.core/storage onyx.core/monitoring] :as event} checkpoint-type
   {:keys [tenancy-id job-id task-id replica-version epoch] :as coordinates}
   slot-id]
  (if coordinates
    (let [bs (checkpoint/read-checkpoint storage tenancy-id job-id replica-version epoch
                                         task-id slot-id checkpoint-type)]
      (.addAndGet ^java.util.concurrent.atomic.AtomicLong (:checkpoint-read-bytes monitoring) (count bs))
      (checkpoint-decompress bs))))

(defn resume-point->coordinates [resume-point]
  (select-keys resume-point [:tenancy-id :job-id :task-id
                             :replica-version :epoch]))

(defn fetch-windows [{:keys [onyx.core/slot-id] :as event} resume-point task-id]
  (->> resume-point
       (vals)
       (remove #(= :initialize (:mode %)))
       (map resume-point->coordinates)
       (distinct)
       (reduce (fn [m resume]
                 (let [checkpoint (read-checkpoint event :windows resume slot-id)]
                   (assoc m resume checkpoint)))
               {})))

(defn lookup-fetched-state [mapping window-id slot-id fetched]
  (if mapping
    (let [{:keys [slot-migration]} mapping
          ;; TODO, use slot-id mappings
          _ (assert (= slot-migration :direct))
          coordinates (resume-point->coordinates mapping)
          state (get fetched coordinates)]
      (when-not (contains? state window-id)
        (throw (ex-info "Stored resume-point missing window state."
                        {:kill-job? false
                         :coordinates coordinates
                         :window/id window-id})))
      (get state window-id))))

(defn state-indexes [windows triggers]
  (into {} 
        (map vector 
             (into (vec (sort (map :window/id windows)))
                   (sort (map (juxt :trigger/id :trigger/window-id) triggers))) 
             (map short (range -32768 32767)))))

(defn recover-windows
  [{:keys [onyx.core/windows onyx.core/triggers onyx.core/task-id onyx.core/slot-id onyx.core/task-map] :as event}
   state-store
   recover-coordinates]
  (let [state-idxes (state-indexes windows triggers)
        resume-mapping (coordinates->windows-resume-point event recover-coordinates)
        fetched (fetch-windows event resume-mapping task-id)]
    (mapv (fn [{:keys [window/id] :as window}]
            (let [resolved (wc/resolve-window-state window 
                                                    triggers 
                                                    state-store 
                                                    state-idxes
                                                    task-map)
                  win-resume-mapping (get resume-mapping id)]
              (if (= :resume (:mode win-resume-mapping))
                resolved
                ;; FIXME 
                ;; FIXME 
                ;; FIXME 
                ;; FIXME 
                ;; FIXME 
                #_(->> (lookup-fetched-state win-resume-mapping id slot-id fetched)
                     (ws/recover-state resolved))
                resolved)))
          windows)))

(defn recover-output [event recover-coordinates]
  (if-let [resume-mapping (coordinates->output-resume-point event recover-coordinates)]
    (let [{:keys [slot-migration]} resume-mapping
          ;; TODO, use slot-id mappings
          _ (assert (= slot-migration :direct))
          {:keys [onyx.core/slot-id]} event]
      (read-checkpoint event :output resume-mapping slot-id))))

(defn recover-input [event recover-coordinates]
  (if-let [resume-mapping (coordinates->input-resume-point event recover-coordinates)]
    (let [{:keys [slot-migration]} resume-mapping
          ;; TODO, use slot-id mappings
          _ (assert (= slot-migration :direct))
          {:keys [onyx.core/slot-id]} event]
      (read-checkpoint event :input resume-mapping slot-id))))
