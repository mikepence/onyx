(ns onyx.state.lmdb
  (:require [onyx.state.protocol.db :as db]
            [onyx.state.serializers.utils :refer [trigger-value-type nil-extent encode-key equals 
                                                  window-type-id trigger-type-id
                                                  window-value-type trigger-value-type]]
            [onyx.state.serializers.windowing-key-encoder :as enc]
            [onyx.state.serializers.windowing-key-decoder :as dec]
            [onyx.compression.nippy :refer [localdb-decompress localdb-compress]])
  (:import [org.fusesource.lmdbjni Database Env Transaction Entry]
           [java.util.concurrent.atomic AtomicLong]
           [org.agrona.concurrent UnsafeBuffer]))

(defn ^Transaction read-txn [^Env env]
   (.createReadTransaction env))

 (defn items
   [^Database db txn]
   (iterator-seq (.iterate db txn)))

(defn db-empty? [db env]
  (let [txn (read-txn env)]
    (try 
     (zero? (count (items db txn)))
     (finally
      (.abort txn)))))

(defn stat [^Database db]
  (let [stat (.stat db)]
    {:ms-entries (.ms_entries stat)
     :ms-psize (.ms_psize stat)
     :ms-overflow-pages (.ms_overflow_pages stat)
     :ms-depth (.ms_depth stat)
     :ms-leaf-pages (.ms_leaf_pages stat)}))

(deftype StateBackend [^Database db ^String name ^Env env ^AtomicLong cnt key-enc key-dec serialize-fn deserialize-fn]
  db/State
  (put-extent! [this window-id group-id extent v]
    (.put db 
          ^bytes (encode-key key-enc window-type-id window-id group-id extent)
          ^bytes (serialize-fn v)))
  (get-extent [this window-id group-id extent]
    (some-> (.get db ^bytes (encode-key key-enc window-type-id window-id group-id extent))
            (deserialize-fn)))
  (delete-extent! [this window-id group-id extent]
    (.delete db ^bytes (encode-key key-enc window-type-id window-id group-id extent)))
  (put-trigger! [this trigger-id group-id v]
    (.put db 
          ^bytes (encode-key key-enc trigger-type-id trigger-id group-id nil-extent)
          ^bytes (serialize-fn v)))
  (get-trigger [this trigger-id group-id]
    (some-> (.get db ^bytes (encode-key key-enc trigger-type-id trigger-id group-id nil-extent))
            (deserialize-fn)))
  (group-id [this group-key]
    (serialize-fn group-key))
  (group-key [this group-id]
    (deserialize-fn group-id))
  (groups [this state-idx]
    ;; FIXME, should be seeking to window-id in here
    ;; For now, full scan will be fine
    (let [txn (read-txn env)]
      (try 
       (->> (items db txn)
            (keep (fn [^Entry entry] 
                    (dec/wrap-impl key-dec (.getKey entry))
                    (when (= (dec/get-state-idx key-dec) state-idx)
                      (deserialize-fn (dec/get-group key-dec))))) 
            (into #{}))
       (finally
        (.abort txn)))))
  (trigger-keys [this]
    (let [txn (read-txn env)]
      (try 
       (let [seek-key (encode-key key-enc trigger-type-id Short/MIN_VALUE (byte-array 0) Long/MIN_VALUE)
             iterator (.seek db txn ^bytes seek-key)
             vs (transient [])] 
         (loop []
           (if (.hasNext iterator)
             (let [entry ^Entry (.next iterator)]
               (dec/wrap-impl key-dec (.getKey entry))
               (when (trigger-value-type entry)
                 (conj! vs [(dec/get-state-idx key-dec)
                            (dec/get-group key-dec)
                            (db/group-key this (dec/get-group key-dec))])
                 (recur)))))
         (persistent! vs))
       (finally 
        (.abort txn)))))
  (group-extents [this window-idx group-id]
    (let [txn (read-txn env)]
      (try 
       (let [seek-key (encode-key key-enc window-type-id window-idx (byte-array 0) Long/MIN_VALUE)
             iterator (.seek db txn ^bytes seek-key)
             vs (transient [])] 
         (loop []
           (if (.hasNext iterator)
             (let [entry ^Entry (.next iterator)]
               (dec/wrap-impl key-dec (.getKey entry))
               (when (and (window-value-type entry)
                          (= (alength ^bytes group-id) (dec/get-group-len key-dec))
                          (equals (dec/get-group key-dec) group-id))
                 (conj! vs (dec/get-extent key-dec))
                 (recur)))))
         (persistent! vs))
       (finally 
        (.abort txn)))))
  (drop! [this]
    (.drop db true))
  ; (export-groups [this]
  ;   (let [txn (read-txn env)]
  ;     (try 
  ;      (->> (items db txn)
  ;           (filter group-value-type)
  ;           (map (fn [^Entry entry]
  ;                  (list (.getKey entry)
  ;                        (.getValue entry))))
  ;           (doall))
  ;      (finally
  ;       (.abort txn)))))
  (export [this]
    (let [txn (read-txn env)]
      (try 
       (->> (items db txn)
            ;; NEED BETTER BYTE FORMAT
            (map (fn [^Entry entry]
                   (list (.getKey entry)
                         (.getValue entry))))
            (doall))
       (finally
        (.abort txn)))))
  (restore! [this stored]
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
        db (.openDatabase env db-name)
        buf1 (UnsafeBuffer. (byte-array 0))
        buf2 (UnsafeBuffer. (byte-array 0))
        dec1 (dec/wrap buf1 0)
        dec2 (dec/wrap buf2 0)
        ;cmp ^java.util.Comparator (onyx.state.lmdb/->Comparator buf1 dec1 buf2 dec2)
        cnt (AtomicLong. Long/MIN_VALUE)
        ;; FIXME
        ;; FIXME
        ;; FIXME
        ;; FIXME
        key-enc-bs (byte-array 8000)
        key-buf (UnsafeBuffer. key-enc-bs)
        key-enc (enc/wrap key-enc-bs key-buf 0)
        key-dec (dec/wrap (UnsafeBuffer. (byte-array 0)) 0)]
    (->StateBackend db name env cnt key-enc key-dec localdb-compress localdb-decompress)))
