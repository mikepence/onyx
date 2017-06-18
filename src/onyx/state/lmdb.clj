(ns onyx.state.lmdb
  (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]])
  (:import [org.fusesource.lmdbjni Database Env]))

(defprotocol State
  (put! [this k v])
  (get! [this k])
  (delete! [this k])
  (drop! [this]))

(defrecord Txn [txn type])

(deftype StateBackend [^org.fusesource.lmdbjni.Database db ^String name ^org.fusesource.lmdbjni.Env env]
  State
  (put! [this k v]
    (.put db k v))
  (get! [this k]
    (.get db k))
  (delete! [this k]
    (.delete db k))
  (drop! [this]
    (.drop db true)))

(defn create-db
  [path db-name max-size]
  (let [env (doto (Env. path)
              (.setMapSize max-size))
        db (.openDatabase env db-name)]
    (->StateBackend db name env)))

(comment 
 (def db (create-db (System/getProperty "java.io.tmpdir") "mydb" 1024000))
 (put! db (messaging-compress "sest") (messaging-compress "siesirnta"))
 (messaging-decompress (get! db (messaging-compress "sest")))

 (defn items
   [db-record txn]
   (let [db   (:db db-record)
         txn* (:txn txn)

         entries (-> db
                     (.iterate txn*)
                     iterator-seq)]
     (map
      (fn [e]
        [(.getKey e) (.getValue e)])
      entries)))

 (defn read-txn
   [env]
   (.createReadTransaction env))

 (defn items-from
   [db txn from]
   (let [entries (-> db 
                     (.seek txn from)
                     iterator-seq)]
     (map (fn [e]
            [(.getKey e) (.getValue e)])
          entries)))


 (def ddd (make-named-db (str (System/getProperty "java.io.tmpdir") "newnew2")
                         "testrepla"))

 (put! ddd 
       (messaging-compress :key) 
       (messaging-compress :value))

 (messaging-decompress (get! ddd (messaging-compress :key)))

 (defn readdddd []
   (let [tx (read-txn ddd)]
     (try 
      (doall (items ddd tx))
      (finally
       (.abort (:txn tx))))) ))
