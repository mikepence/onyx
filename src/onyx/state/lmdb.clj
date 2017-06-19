(ns onyx.state.lmdb
  (:require [onyx.state.protocol.db :as db]
            [onyx.compression.nippy :refer [localdb-decompress localdb-compress]])
  (:import [org.fusesource.lmdbjni Database Env Transaction Entry]))

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

(defn entry->window-id [^Entry entry deserialize-fn]
  (first (deserialize-fn (.getKey entry))))

(defn entry->group [^Entry entry deserialize-fn]
  (second (deserialize-fn (.getKey entry))))

(defn entry->extent [^Entry entry deserialize-fn]
  (let [[_ _ _ extent] (deserialize-fn (.getKey entry))]
    extent))

(defn entry->type [^Entry entry deserialize-fn]
  (let [[_ _ t] (deserialize-fn (.getKey entry))]
    t))

(defn db-empty? [db env]
  (let [txn (read-txn env)]
    (try 
     (zero? (count (items db txn)))
     (finally
      (.abort txn)))))

(deftype StateBackend [^Database db ^String name ^Env env serialize-fn deserialize-fn]
  db/State
  (put-extent! [this window-id group extent v]
    (.put db 
          ^bytes (serialize-fn [window-id group :extent extent])
          ^bytes (serialize-fn v)))
  (get-extent [this window-id group extent]
    (some-> (.get db ^bytes (serialize-fn [window-id group :extent extent]))
            (deserialize-fn)))
  (delete-extent! [this window-id group extent]
    (.delete db ^bytes (serialize-fn [window-id group :extent extent])))
  (put-trigger! [this window-id trigger-id group v]
    (.put db 
          ^bytes (serialize-fn [window-id group :trigger trigger-id])
          ^bytes (serialize-fn v)))
  (get-trigger [this window-id trigger-id group]
    (some-> (.get db ^bytes (serialize-fn [window-id group :trigger trigger-id]))
            (deserialize-fn)))
  (groups [this window-id]
    (let [txn (read-txn env)]
      (try 
       (->> (items db txn)
            (map #(entry->group % deserialize-fn))
            (distinct)
            (doall))
       (finally
        (.abort txn)))))
  (group-extents [this window-id group]
    (let [txn (read-txn env)]
      (try 
       (->> (items db txn)
            (filter (fn [^Entry entry]
                      (and (= window-id (entry->window-id entry deserialize-fn))
                           (= group (entry->group entry deserialize-fn))
                           (= :extent (entry->type entry deserialize-fn)))))
            (map (fn [e] (entry->extent e deserialize-fn)))
            ;; FIXME shouldn't need to sort here, comparator should cover it
            (sort)
            ;; Shouldn't need distinct here as it should be sorted by the cmp, and we short circuit
            (distinct)
            (doall))
       (finally
        (.abort txn)))))
  (drop! [this]
    (.drop db true))
  (export [this window-id]
    (let [txn (read-txn env)]
      (try 
       (->> (items db txn)
            (filter (fn [^Entry entry]
                      (= window-id (entry->window-id entry deserialize-fn))))
            ;; switch to flat array
            (map (fn [^Entry entry]
                   (list (.getKey entry)
                         (.getValue entry))))
            (doall))
       (finally
        (.abort txn)))))

  (restore! [this _ stored]
    (when-not (db-empty? db env)
      (throw (Exception. "LMDB db is not empty. This should never happen.")))
    (run! (fn [[^bytes k ^bytes v]]
            (.put db k v))
          stored)))

(defn create-db
  [peer-config db-name]
  (let [max-size 1024000
        path (System/getProperty "java.io.tmpdir")
        env (doto (Env. path)
              (.setMapSize max-size))
        db (.openDatabase env db-name)]
    (->StateBackend db name env localdb-compress localdb-decompress)))
