(ns onyx.state.state-store-test
  (:require [clojure.test :refer [is deftest]]
            [onyx.state.protocol.db :as s]
            [onyx.state.lmdb]
            [onyx.state.memory]))

(deftest basic-state-store-test
  (let [window-id :window1
        db-name (str (java.util.UUID/randomUUID))
        store (onyx.state.memory/create-db {} :state-id-1)
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
     (finally
      
      
      )
     
     )))

(deftest basic-state-export-restore-test
  (let [window-id :window1
        store (onyx.state.memory/create-db {} :state-id-1)]
    (try
     ;; actions
     (s/put-extent! store window-id :group1 3 :my-value1)
     (s/put-extent! store window-id :group2 5 :my-value2)
     (s/put-extent! store window-id :group2 6 :my-value3)
     (let [exported (s/export store window-id)
           store2 (onyx.state.memory/create-db {} :state-id-2)]
       (try
        (s/restore! store2 window-id exported)
        (is (= (s/groups store window-id) (s/groups store2 window-id)))
        (is (= :my-value1 (s/get-extent store2 window-id :group1 3)))
        (is (= :my-value2 (s/get-extent store window-id :group2 5)))
        (is (= :my-value3 (s/get-extent store window-id :group2 6)))
        (is (nil? (s/get-extent store window-id :group2 7)))

        (finally (s/drop! store2))))
     (finally (s/drop! store)))))
