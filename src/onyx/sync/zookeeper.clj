(ns ^:no-doc onyx.sync.zookeeper
  (:require [clojure.data.fressian :as fressian]
            [com.stuartsierra.component :as component]
            [taoensso.timbre]
            [onyx.extensions :as extensions]
            [zookeeper :as zk]
            [zookeeper.util :as util])
  (:import [java.util UUID]
           [org.apache.curator.test TestingServer]))

(def root-path "/onyx")

(defn prefix-path [prefix]
  (str root-path "/" prefix))

(defn peer-path [prefix]
  (str root-path "/" prefix "/peer"))

(defn peer-state-path [prefix]
  (str root-path "/" prefix "/peer-state"))

(defn pulse-path [prefix]
  (str root-path "/" prefix "/pulse"))

(defn payload-path [prefix]
  (str root-path "/" prefix "/payload"))

(defn ack-path [prefix]
  (str root-path "/" prefix "/ack"))

(defn exhaust-path [prefix]
  (str root-path "/" prefix "/exhaust"))

(defn seal-path [prefix]
  (str root-path "/" prefix "/seal"))

(defn completion-path [prefix]
  (str root-path "/" prefix "/completion"))

(defn cooldown-path [prefix]
  (str root-path "/" prefix "/cooldown"))

(defn status-path [prefix]
  (str root-path "/" prefix "/status"))

(defn catalog-path [prefix]
  (str root-path "/" prefix "/catalog"))

(defn workflow-path [prefix]
  (str root-path "/" prefix "/workflow"))

(defn shutdown-path [prefix]
  (str root-path "/" prefix "/shutdown"))

(defn job-path [prefix]
  (str root-path "/" prefix "/job"))

(defn task-path [prefix subpath]
  (str root-path "/" prefix "/task/" subpath))

(defn job-log-path [prefix]
  (str root-path "/" prefix "/job-log"))

(defn born-log-path [prefix]
  (str root-path "/" prefix "/coordinator/born-log"))

(defn death-log-path [prefix]
  (str root-path "/" prefix "/coordinator/death-log"))

(defn planning-log-path [prefix]
  (str root-path "/" prefix "/coordinator/planning-log"))

(defn ack-log-path [prefix]
  (str root-path "/" prefix "/coordinator/ack-log"))

(defn evict-log-path [prefix]
  (str root-path "/" prefix "/coordinator/evict-log"))

(defn offer-log-path [prefix]
  (str root-path "/" prefix "/coordinator/offer-log"))

(defn revoke-log-path [prefix]
  (str root-path "/" prefix "/coordinator/revoke-log"))

(defn exhaust-log-path [prefix]
  (str root-path "/" prefix "/coordinator/exhaust-log"))

(defn seal-log-path [prefix]
  (str root-path "/" prefix "/coordinator/seal-log"))

(defn complete-log-path [prefix]
  (str root-path "/" prefix "/coordinator/complete-log"))

(defn shutdown-log-path [prefix]
  (str root-path "/" prefix "/coordinator/shutdown-log"))

(defrecord ZooKeeper [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting ZooKeeper")
    (let [server (when (:zookeeper/server? opts) (TestingServer. (:zookeeper.server/port opts)))
          conn (zk/connect (:zookeeper/address opts))
          prefix (:onyx/id opts)]
      (zk/create conn root-path :persistent? true)
      (zk/create conn (prefix-path prefix) :persistent? true)
      (zk/create conn (peer-path prefix) :persistent? true)
      (zk/create conn (peer-state-path prefix) :persistent? true)
      (zk/create conn (pulse-path prefix) :persistent? true)
      (zk/create conn (payload-path prefix)  :persistent? true)
      (zk/create conn (ack-path prefix) :persistent? true)
      (zk/create conn (exhaust-path prefix) :persistent? true)
      (zk/create conn (seal-path prefix) :persistent? true)
      (zk/create conn (completion-path prefix) :persistent? true)
      (zk/create conn (cooldown-path prefix) :persistent? true)
      (zk/create conn (status-path prefix) :persistent? true)
      (zk/create conn (catalog-path prefix) :persistent? true)
      (zk/create conn (workflow-path prefix) :persistent? true)
      (zk/create conn (shutdown-path prefix) :persistent? true)
      (zk/create conn (job-path prefix) :persistent? true)
      (zk/create conn (job-log-path prefix) :persistent? true)

      (zk/create-all conn (born-log-path) :persistent? true)
      (zk/create-all conn (death-log-path) :persistent? true)
      (zk/create-all conn (planning-log-path) :persistent? true)
      (zk/create-all conn (ack-log-path) :persistent? true)
      (zk/create-all conn (evict-log-path) :persistent? true)
      (zk/create-all conn (offer-log-path) :persistent? true)
      (zk/create-all conn (revoke-log-path) :persistent? true)
      (zk/create-all conn (exhaust-log-path) :persistent? true)
      (zk/create-all conn (seal-log-path) :persistent? true)
      (zk/create-all conn (complete-log-path) :persistent? true)
      (zk/create-all conn (shutdown-log-path) :persistent? true)

      (assoc component :server server :conn conn :prefix (:onyx/id opts))))

  (stop [component]
    (taoensso.timbre/info "Stopping ZooKeeper")
    (zk/close (:conn component))

    (when (:server component)
      (.stop (:server component)))

    component))

(defn zookeeper [opts]
  (map->ZooKeeper {:opts opts}))

(defn trailing-id [s]
  (last (clojure.string/split s #"/")))

(defn serialize-edn [x]
  (.array (fressian/write x)))

(defn deserialize-edn [x]
  (fressian/read x))

(defmethod extensions/create [ZooKeeper :peer]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (peer-path prefix) "/" uuid)]
    (zk/create (:conn sync) node :persistent? true)
    {:node node :uuid uuid}))

(defmethod extensions/create [ZooKeeper :pulse]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (pulse-path prefix) "/" uuid)]
    (zk/create (:conn sync) node :persistent? false)
    {:node node :uuid uuid}))

(defmethod extensions/create [ZooKeeper :payload]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (payload-path prefix) "/" uuid)]
    (zk/create (:conn sync) node :persistent? true)
    {:node node :uuid uuid}))

(defmethod extensions/create [ZooKeeper :ack]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (ack-path prefix) "/" uuid)]
    (zk/create (:conn sync) node :persistent? true)
    {:node node :uuid uuid}))

(defmethod extensions/create [ZooKeeper :exhaust]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (exhaust-path prefix) "/" uuid)]
    (zk/create (:conn sync) node :persistent? true)
    {:node node :uuid uuid}))

(defmethod extensions/create [ZooKeeper :seal]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (seal-path prefix) "/" uuid)]
    (zk/create (:conn sync) node :persistent? true)
    {:node node :uuid uuid}))

(defmethod extensions/create [ZooKeeper :completion]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (completion-path prefix) "/" uuid)]
    (zk/create (:conn sync) node :persistent? true)
    {:node node :uuid uuid}))

(defmethod extensions/create [ZooKeeper :cooldown]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (cooldown-path prefix) "/" uuid)]
    (zk/create (:conn sync) node :persistent? true)
    {:node node :uuid uuid}))

(defmethod extensions/create [ZooKeeper :status]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (status-path prefix) "/" uuid)]
    (zk/create (:conn sync) node :persistent? true)
    {:node node :uuid uuid}))

(defmethod extensions/create [ZooKeeper :catalog]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (catalog-path prefix) "/" uuid)]
    (zk/create (:conn sync) node :persistent? true)
    {:node node :uuid uuid}))

(defmethod extensions/create [ZooKeeper :workflow]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (workflow-path prefix) "/" uuid)]
    (zk/create (:conn sync) node :persistent? true)
    {:node node :uuid uuid}))

(defmethod extensions/create [ZooKeeper :shutdown]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (shutdown-path prefix) "/" uuid)]
    (zk/create (:conn sync) node :persistent? true)
    {:node node :uuid uuid}))

(defmethod extensions/create [ZooKeeper :task]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        uuid (UUID/randomUUID)
        node (str (task-path prefix uuid) "/task-")]
    {:node (zk/create-all (:conn sync) node :persistent? true :sequential? true)
     :uuid uuid}))

(defmethod extensions/create [ZooKeeper :job]
  [sync _ subpath]
  (let [prefix (:onyx/id (:opts sync))
        node (str (job-path prefix) "/" subpath)]
    {:node (zk/create (:conn sync) node :persistent? true)}))

(defmethod extensions/create [ZooKeeper :job-log]
  [sync _ content]
  (let [prefix (:onyx/id (:opts sync))
        node (str (job-log-path prefix) "/offer-")
        data (serialize-edn content)]
    {:node (zk/create (:conn sync) node :data data :persistent? true :sequential? true)}))

(defmethod extensions/create-node ZooKeeper
  [sync node]
  (zk/create (:conn sync) node :persistent? true))

(defmethod extensions/create-at [ZooKeeper :peer-state]
  [sync _ subpath content]
  (let [prefix (:onyx/id (:opts sync))
        node (str (peer-state-path prefix) "/" subpath "/state-")
        data (serialize-edn content)]
    {:node (zk/create-all (:conn sync) node :data data :persistent? true :sequential? true)}))

(defmethod extensions/create-at [ZooKeeper :workflow]
  [sync _ job-id content]
  (let [prefix (:onyx/id (:opts sync))
        data (serialize-edn content)
        node (str (workflow-path prefix) "/" job-id)]
    {:node (zk/create (:conn sync) node :data data :persistent? true)}))

(defmethod extensions/create-at [ZooKeeper :catalog]
  [sync _ job-id content]
  (let [prefix (:onyx/id (:opts sync))
        data (serialize-edn content)
        node (str (catalog-path prefix) "/" job-id)]
    {:node (zk/create (:conn sync) node :data data :persistent? true)}))

(defmethod extensions/create-at [ZooKeeper :task]
  [sync _ subpath content]
  (let [prefix (:onyx/id (:opts sync))
        data (serialize-edn content)
        node (str (task-path prefix subpath) "/task-")]
    {:node (zk/create-all (:conn sync) node :data data :persistent? true :sequential? true)}))

(defmethod extensions/bucket [ZooKeeper :peer-state]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        children (or (zk/children (:conn sync) (peer-state-path prefix)) [])]
    (map #(str (peer-state-path prefix) "/" %) children)))

(defmethod extensions/bucket [ZooKeeper :job]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        children (or (zk/children (:conn sync) (job-path prefix)) [])]
    (map #(str (job-path prefix) "/" %) children)))

(defmethod extensions/bucket [ZooKeeper :job-log]
  [sync _]
  (let [prefix (:onyx/id (:opts sync))
        children (or (zk/children (:conn sync) (job-log-path prefix)) [])]
    (map #(str (job-log-path prefix) "/" %) children)))

(defmethod extensions/bucket-at [ZooKeeper :task]
  [sync _ subpath]
  (let [prefix (:onyx/id (:opts sync))
        job-id (trailing-id subpath) 
        children (or (zk/children (:conn sync) (task-path prefix job-id)) [])]
    (map #(str (task-path prefix job-id) "/" %) children)))

(defmethod extensions/resolve-node [ZooKeeper :peer]
  [sync _ subpath]
  (let [prefix (:onyx/id (:opts sync))]
    (str (peer-path prefix) "/" subpath)))

(defmethod extensions/resolve-node [ZooKeeper :peer-state]
  [sync _ subpath & more]
  (let [prefix (:onyx/id (:opts sync))]
    (str (peer-state-path prefix) "/" subpath)))

(defmethod extensions/resolve-node [ZooKeeper :job-log]
  [sync _ & more]
  (let [prefix (:onyx/id (:opts sync))]
    (job-log-path prefix)))

(defmethod extensions/resolve-node [ZooKeeper :job]
  [sync _ job-id & more]
  (let [prefix (:onyx/id (:opts sync))]
    (str (job-path prefix) "/" job-id)))

(defmethod extensions/resolve-node [ZooKeeper :task]
  [sync _ job-node & more]
  (let [prefix (:onyx/id (:opts sync))]
    (task-path prefix (trailing-id job-node))))

(defmethod extensions/children ZooKeeper
  [sync node]
  (let [children (or (zk/children (:conn sync) node) [])]
    (map #(str node "/" %) children)))

(defmethod extensions/delete ZooKeeper
  [sync node] (zk/delete (:conn sync) node))

(defmethod extensions/write-node ZooKeeper
  [sync node contents]
  (let [version (:version (zk/exists (:conn sync) node))]
    (zk/set-data (:conn sync) node (serialize-edn contents) version)))

(defmethod extensions/touch-node ZooKeeper
  [sync node]
  (let [contents (zk/data (:conn sync) node)]
    (zk/set-data (:conn sync) node (:data contents)
                 (:version (:stat contents)))))

(defmethod extensions/read-node ZooKeeper
  [sync node]
  (deserialize-edn (:data (zk/data (:conn sync) node))))

(defmethod extensions/read-node-at [ZooKeeper :task]
  [sync _ & subpaths]
  (let [prefix (:onyx/id (:opts sync))
        job-id (first subpaths)
        task-id (second subpaths)]
    (extensions/read-node sync (str (task-path prefix job-id) "/" task-id))))

(defmethod extensions/dereference ZooKeeper
  [sync node]
  (let [prefix (:onyx/id (:opts sync))
        children (or (zk/children (:conn sync) node) [])
        sorted-children (util/sort-sequential-nodes children)]
    (when (seq sorted-children)
      (let [path  (str node "/" (last sorted-children))]
        {:node path :content (extensions/read-node sync path)}))))

(defmethod extensions/previous-node ZooKeeper
  [sync node]
  (let [id (util/extract-id node)
        prev-id (dec id)
        ;;; Handle decrementing across orders of magnitude and not losing a 0.
        prev-str (if (< (count (str prev-id)) (count (str id)))
                   (str "0" prev-id)
                   (str prev-id))]
    ;;; Avoid overwriting anything else in the path by prefixing "state-"
    (clojure.string/replace node (str "state-" id) (str "state-" prev-str))))

(defmethod extensions/node-exists? ZooKeeper
  [sync node]
  (boolean (zk/exists (:conn sync) node)))

(defmethod extensions/version ZooKeeper
  [sync node]
  (:version (zk/exists (:conn sync) node)))

(defmethod extensions/creation-time ZooKeeper
  [sync node]
  (:ctime (zk/exists (:conn sync) node)))

(defmethod extensions/on-change ZooKeeper
  [sync node cb]
  (let [f (fn [event]
            (when (= (:event-type event) :NodeDataChanged)
              (cb event)))]
    (zk/exists (:conn sync) node :watcher f)))

(defmethod extensions/on-child-change ZooKeeper
  [sync node cb]
  (let [f (fn [event]
            (when (= (:event-type event) :NodeChildrenChanged)
              (cb event)))]
    (zk/children (:conn sync) node :watcher f)))

(defmethod extensions/on-delete ZooKeeper
  [sync node cb]
  (let [f (fn [event]
            (when (= (:event-type event) :NodeDeleted)
              (cb event)))]
    (zk/exists (:conn sync) node :watcher f)))

