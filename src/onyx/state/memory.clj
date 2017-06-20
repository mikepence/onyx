(ns onyx.state.memory
  (:require [onyx.state.protocol.db :as db]
            [onyx.compression.nippy :refer [localdb-compress localdb-decompress]]))

(deftype StateBackend [state serialize-fn deserialize-fn]
  db/State
  (put-extent! [this window-id group extent v]
    (swap! state assoc-in [window-id group extent] v))
  (get-extent [this window-id group extent]
    (get-in @state [window-id group extent]))
  (delete-extent! [this window-id group extent]
    (swap! state update-in [window-id group] dissoc extent))
  (put-trigger! [this trigger-id group v]
    (swap! state assoc-in [trigger-id group] v))
  (get-trigger [this trigger-id group]
    (get-in @state [trigger-id group]))
  (groups [this window-id]
    (keys (get @state window-id)))
  (group-extents [this window-id group]
    (keys (get-in @state [window-id group :window])))
  (drop! [this]
    (reset! state nil))
  (export [this window-id]
    (localdb-compress (get @state window-id)))
  (restore! [this window-id bs]
    (swap! state assoc window-id (localdb-decompress bs))))

(defn create-db
  [peer-config _]
  (->StateBackend (atom {}) localdb-compress localdb-decompress))
