(ns onyx.state.lmdb
  (:require [onyx.state.protocol.db :as db]
            [onyx.compression.nippy :refer [localdb-decompress localdb-compress]])
  (:import [org.fusesource.lmdbjni Database Env Transaction]))

(defn ^Transaction read-txn [^Env env]
   (.createReadTransaction env))

 (defn items
   [^Database db txn]
   (iterator-seq (.iterate db txn)))

; (defn items-from
;   [db txn from]
;   (let [entries (-> db 
;                     (.seek txn from)
;                     iterator-seq)]
;     (map (fn [e]
;            [(.getKey e) (.getValue e)])
;          entries)))


(defn readdddd []
   )

(deftype StateBackend [^Database db ^String name ^Env env serialize-fn deserialize-fn]
  db/State
  (put-extent! [this window-id group extent v]
    (.put db 
          ^bytes (serialize-fn [window-id group extent])
          ^bytes (serialize-fn v)))
  (get-extent [this window-id group extent]
    (.get db ^bytes (serialize-fn [window-id group extent])))
  (delete-extent! [this window-id group extent]
    (.delete db ^bytes (serialize-fn [window-id group extent])))
  (put-trigger! [this window-id trigger-id group v]
    (.put db 
          ^bytes (serialize-fn [window-id group :trigger trigger-id])
          ^bytes (serialize-fn v)))
  (get-trigger [this window-id trigger-id group]
    (.get db ^bytes (serialize-fn [window-id group :trigger trigger-id])))
  (groups [this window-id]
    (let [tx (read-txn db)]
      (try 
       (doall (map (fn [i]
                     (println "IIS" (type i))
                     [(deserialize-fn (.getKey i)) (deserialize-fn (.getValue i))]) 
                   (items db tx)))
       (finally
        (.abort (:txn tx))))))
  (group-extents [this window-id group]
    ;(keys (get-in @state [window-id group :window]))
    )
  (drop! [this]
    (.drop db true))
  (export [this window-id]
    ; (localdb-compress (get @state window-id))
    )
  (restore! [this window-id bs]
    ; (swap! state assoc window-id (localdb-decompress bs))
    
    ))

(defn create-db
  [peer-config db-name]
  (let [max-size 1024000
        path (System/getProperty "java.io.tmpdir")
        env (doto (Env. path)
              (.setMapSize max-size))
        db (.openDatabase env db-name)]
    (->StateBackend db name env)))

(comment 
 (def db (create-db (System/getProperty "java.io.tmpdir") "mydb" 1024000))
 (put! db (messaging-compress "sest") (messaging-compress "siesirnta"))
 (messaging-decompress (get! db (messaging-compress "sest")))

 

 (def ddd (make-named-db (str (System/getProperty "java.io.tmpdir") "newnew2")
                         "testrepla"))

 (put! ddd 
       (messaging-compress :key) 
       (messaging-compress :value))

 (messaging-decompress (get! ddd (messaging-compress :key)))

 )
