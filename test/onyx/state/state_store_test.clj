(ns onyx.state.state-store-test
  (:require [clojure.test :refer [is deftest]]
            [onyx.state.protocol.db :as s]
            [onyx.state.lmdb]
            [onyx.state.memory]))

;; FIXME copy in stored S3 file, convert to new format

(deftest basic-state-store-test
  (let [window-id :window1
        db-name (str (java.util.UUID/randomUUID))
        ;store (onyx.state.memory/create-db {} :state-id-1)
        store (onyx.state.lmdb/create-db {} db-name)
        ]
    (try
     ;; actions
     (s/put-extent! store window-id :group1 3 :my-value1)
     (s/put-extent! store window-id :group2 5 :my-value2)
     (s/put-extent! store window-id :group2 6 :my-value3)
     (s/delete-extent! store window-id :group2 6)

     ;; results
     (is (= :my-value1 (s/get-extent store window-id :group1 3)))
     (is (= :my-value2 (s/get-extent store window-id :group2 5)))
     (is (nil? (s/get-extent store window-id :group2 6)))
     (finally (s/drop! store)))))

(deftest basic-state-export-restore-test
  (let [db-name (str (java.util.UUID/randomUUID))
        window-id :window1
        store (onyx.state.lmdb/create-db {} db-name)
        store-mem (onyx.state.memory/create-db {} db-name)
        ]
    (try
     (doseq [s [store store-mem]]
       ;; actions
       (s/put-extent! s window-id :group1 3 :my-value1)
       (s/put-extent! s window-id :group2 5 :my-value2)
       (s/put-extent! s window-id :group2 6 :my-value3))
     (is (= (sort (s/groups store window-id)) 
            (sort (s/groups store-mem window-id))))

     (let [exported (s/export store window-id)
           ;store2 (onyx.state.memory/create-db {} :state-id-2)
           db-name (str (java.util.UUID/randomUUID))
           store2 (onyx.state.lmdb/create-db {} db-name)]
       (try
        (s/restore! store2 window-id exported)
        (is (= (s/groups store window-id) 
               (s/groups store2 window-id)))
        (is (= :my-value1 (s/get-extent store2 window-id :group1 3)))
        (is (= :my-value2 (s/get-extent store window-id :group2 5)))
        (is (= :my-value3 (s/get-extent store window-id :group2 6)))
        (is (nil? (s/get-extent store window-id :group2 7)))
        (finally (s/drop! store2))))
     (finally (s/drop! store)))))
