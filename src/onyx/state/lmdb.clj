(ns onyx.state.lmdb
  (:require [onyx.state.protocol.db :as db]
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

(deftype Comparator [^UnsafeBuffer buf1 decoder1 ^UnsafeBuffer buf2 decoder2]
  java.util.Comparator
  (compare [this o1 o2]
    (.wrap buf1 ^bytes o1)
    (.wrap buf2 ^bytes o2)
    (let [i (- (dec/get-state-idx decoder2)
               (dec/get-state-idx decoder1))]
      (if (zero? i)
        (let [g (- (dec/get-group decoder2) 
                   (dec/get-group decoder1))]
          (if (zero? g)
            (- (dec/get-extent decoder2)
               (dec/get-extent decoder1))
            g)) 
        i))))

(def ^:constant group-key-type-id (byte 0))
(def ^:constant window-key-type-id (byte 1))
(def ^:constant trigger-key-type-id (byte 2))

(defn group-value-type [^Entry e]
  (= group-key-type-id (aget (.getKey e) 0)))

(defn group-bytes->key [^bytes bs]
  (let [group-key-bs (byte-array (inc (alength bs)))
        buf (UnsafeBuffer. group-key-bs)
        _ (.putByte buf 0 group-key-type-id)
        _ (.putBytes buf 1 bs)]
    group-key-bs))

(defn dedupe-group-id [^Database db ^AtomicLong cnt ^bytes bs]
  (let [group-key-bs (group-bytes->key bs)
        g (.get db ^bytes group-key-bs)]
    (if g
      g
      (let [group-id (.incrementAndGet cnt)
            group-bs (byte-array 8)
            buf (UnsafeBuffer. group-bs)]
        (.putLong buf 0 group-id)
        (.put db ^bytes group-key-bs group-bs)
        group-bs))))

(defn ^bytes encode-key [db cnt key-enc idx group-bs extent]
  (println "IDX" idx)
  (enc/set-type key-enc window-key-type-id)
  (enc/set-state-idx key-enc idx)
  (enc/set-group-bs key-enc (dedupe-group-id db cnt group-bs))
  (enc/set-extent key-enc extent)
  (println "FINALTYPE" (aget (enc/bs key-enc) 0))
  (enc/bs key-enc))

(def no-extent -1)

(deftype StateBackend [^Database db ^String name ^Env env ^AtomicLong cnt key-enc key-dec serialize-fn deserialize-fn]
  db/State
  (put-extent! [this window-id group extent v]
    (println "PUTTINGGROUP" group)
    (.put db 
          ^bytes (encode-key db cnt key-enc window-id (serialize-fn group) extent)
          ^bytes (serialize-fn v)))
  (get-extent [this window-id group extent]
    (some-> (.get db ^bytes (encode-key db cnt key-enc window-id (serialize-fn group) extent))
            (deserialize-fn)))
  (delete-extent! [this window-id group extent]
    (.delete db ^bytes (encode-key db cnt key-enc window-id (serialize-fn group) extent)))
  (put-trigger! [this trigger-id group v]
    (.put db 
          ^bytes (encode-key db cnt key-enc trigger-id (serialize-fn group) no-extent)
          ^bytes (serialize-fn v)))
  (get-trigger [this trigger-id group]
    (some-> (.get db ^bytes (encode-key db cnt key-enc trigger-id (serialize-fn group) no-extent))
            (deserialize-fn)))
  (groups [this window-id]
    ;; FIXME, should be seeking to window-id in here
    (let [txn (read-txn env)]
      (try 
       (->> (items db txn)
            (keep (fn [^Entry entry] 
                    (when-not (group-value-type entry)
                      (dec/wrap-impl key-dec (.getKey entry))
                      (dec/get-group key-dec)))) 
            (into #{}))
       (finally
        (.abort txn)))))
  (group-extents [this window-id group]
    (let [txn (read-txn env)]
      (try 
       (->> (items db txn)
            (filter (fn [^Entry entry]
                      (when-not (group-value-type entry)
                        (dec/wrap-impl key-dec (.getKey entry))
                        (and (= window-id (dec/get-state-idx key-dec))
                             (= group (dec/get-group key-dec))))))
            (map (fn [^Entry entry] 
                   (dec/wrap-impl key-dec (.getKey entry))
                   (dec/get-extent entry)))
            ;; FIXME shouldn't need to sort here, comparator should cover it
            (sort)
            ;; Shouldn't need distinct here as it should be sorted by the cmp, and we short circuit
            (distinct)
            (doall))
       (finally
        (.abort txn)))))
  (drop! [this]
    (.drop db true))
  (export-groups [this]
    (let [txn (read-txn env)]
      (try 
       (->> (items db txn)
            (filter group-value-type)
            (map (fn [^Entry entry]
                   (list (.getKey entry)
                         (.getValue entry))))
            (doall))
       (finally
        (.abort txn)))))
  (export-state [this state-idx]
    (let [txn (read-txn env)]
      (try 
       (->> (items db txn)
            (filter (fn [^Entry entry]
                      (when-not (group-value-type entry)
                        (dec/wrap-impl key-dec (.getKey entry))
                        (= state-idx (dec/get-state-idx key-dec)))))
            ;; switch to flat array
            (map (fn [^Entry entry]
                   (list (.getKey entry)
                         (.getValue entry))))
            (doall))
       (finally
        (.abort txn)))))
  ;; Should take already translated group-ids?
  (restore! [this stored]
    #_(when-not (db-empty? db env)
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
        cmp ^java.util.Comparator (onyx.state.lmdb/->Comparator buf1 dec1 buf2 dec2)
        cnt (AtomicLong. Long/MIN_VALUE)
        key-enc-bs (byte-array 19)
        key-buf (UnsafeBuffer. key-enc-bs)
        key-enc (enc/wrap key-enc-bs key-buf 0)
        key-dec (dec/wrap (UnsafeBuffer. (byte-array 0)) 0)
        
        ]
    (->StateBackend db name env cnt key-enc key-dec localdb-compress localdb-decompress)))
