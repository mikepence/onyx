(ns onyx.state.lmdb
  (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]])
  (:import [org.fusesource.lmdbjni Database Env]))

;; FIXME, lots of copied code from clj-lmdb. Clean up. Rip out / add licensing attribution.

(defrecord NamedDB [env db name])
(defrecord Txn [txn type])

(defn make-named-db
  "Create a named database using an env.
   Returns a db record you can use with all
   the other functions"
  ([dir-path name max-size]
   (let [env (doto (Env. dir-path)
               (.setMapSize max-size))
         db  (.openDatabase env name)]
     (NamedDB. env
               db
               name)))
  ([dir-path name]
   (make-named-db dir-path
                  name
                  10485760)))

(defn put!
  ([db-record txn k v]
   (let [db (:db db-record)]
     (.put db
           (:txn txn)
           k
           v)))

  ([db-record k v]
   (let [db (:db db-record)]
     (.put db
           k
           v))))

(defn get!
  ([db-record txn k]
   (let [db (:db db-record)]
     (.get db
           (:txn txn)
           k)))
  
  ([db-record k]
   (let [db (:db db-record)]
     (.get db
           k))))

(defn delete!
  ([db-record txn k]
   (let [db (:db db-record)]
     (.delete db
              (:txn txn)
              k)))

  ([db-record k]
   (let [db (:db db-record)]
    (.delete db
             k))))

(defn drop-db!
  [named-db-record]
  (-> named-db-record
      :db
      (.drop true)))

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
  [db-record]
  (let [env (:env db-record)
        txn (.createReadTransaction env)]
    (Txn. txn :read)))

(defn items-from
  [db-record txn from]
  (let [db   (:db db-record)
        txn* (:txn txn)

        entries (-> db
                    (.seek txn*
                           from)
                    iterator-seq)]
    (map
     (fn [e]
       [(.getKey e) (.getValue e)])
     entries)))



(comment 
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

