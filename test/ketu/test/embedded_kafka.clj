(ns ketu.test.embedded-kafka
  (:require [clojure.java.io]
            [clojure.string :as str]
            [ketu.test.chain-assoc :as ctx])
  (:import (java.io File)
           (java.net InetSocketAddress)
           (java.time LocalDateTime)
           (java.util Properties)
           (org.apache.commons.io FileUtils)
           (org.apache.zookeeper.server ZooKeeperServer NIOServerCnxnFactory)
           (kafka.server KafkaServerStartable)))

;; Mostly copied from https://github.com/RiadVargas/clj-embedded-kafka

;; Util

(defn- system-tmpdir [] (System/getProperty "java.io.tmpdir"))

(defn- now-folder []
  (-> (LocalDateTime/now)
      (hash)
      (mod Integer/MAX_VALUE)
      (str)))

(def tmp-dir-parent "clj-kafka-unit")

(defn- tmp-dir-path ^File [kind]
  (clojure.java.io/file (system-tmpdir) tmp-dir-parent kind (now-folder)))

(defn- clean-tmp-path! [kind]
  (-> (clojure.java.io/file (system-tmpdir) tmp-dir-parent kind)
      (FileUtils/deleteDirectory)))

;; Zookeeper

(defn- default-zk-opts
  []
  {:data-dir (tmp-dir-path "zookeeper")
   :log-dir (tmp-dir-path "zookeeper")
   :tick-time 3000
   :port 2181
   :max-cc 10})

(defn start-zookeeper!
  [& [opts]]
  (let [final-opts (merge (default-zk-opts) opts)
        {:keys [data-dir log-dir tick-time port max-cc]} final-opts
        zk-server (ZooKeeperServer. data-dir log-dir tick-time)]
    (doto (NIOServerCnxnFactory.)
      (.configure (InetSocketAddress. port) max-cc)
      (.startup zk-server))))

(defn stop-zookeeper!
  [^NIOServerCnxnFactory server]
  (.shutdown server)
  (clean-tmp-path! "zookeeper"))

;; Kafka

(defn- default-kafka-opts
  [& [{::keys [zk-port] :or {zk-port 2181}}]]
  {:log.dir (-> "kafka" tmp-dir-path .getPath)
   :zookeeper.connect (format "127.0.0.1:%d" zk-port)
   :auto.create.topics.enable "true"
   :log.flush.interval.messages "1"
   :offsets.topic.replication.factor "1"
   :listeners "PLAINTEXT://127.0.0.1:9999"
   :broker.id "0"})

(defn- ->kafka-config [config]
  (->> config
       (map (fn [[k v]] (vector (-> k name (str/replace "-" ".")) v)))
       (into {})))

(defn- ->props
  [m]
  (let [props (Properties.)]
    (run! (fn [[k v]] (.setProperty props k v)) m)
    props))

(defn start-kafka!
  [& [opts]]
  (let [props (-> (default-kafka-opts)
                  (merge opts)
                  ->kafka-config
                  ->props)]
    (doto (KafkaServerStartable/fromProps props)
      (.startup))))

(defn stop-kafka!
  [^KafkaServerStartable server]
  (.shutdown server)
  (.awaitShutdown server))

;; Context execution

(defn stop-standalone!
  "Stops embedded kafka and zk"
  [{::keys [zookeeper kafka]}]
  (when kafka
    (stop-kafka! kafka))
  (when zookeeper
    (stop-zookeeper! zookeeper)))

(defn start-standalone!
  "Starts embedded kafka and zk"
  [zk-opts kafka-opts]
  (ctx/chain-assoc
    {}
    (fn on-error [ctx _error] (stop-standalone! ctx))
    ::zookeeper (fn [_] (start-zookeeper! zk-opts))
    ::kafka (fn [_] (start-kafka! kafka-opts))))

(defn with-embedded-kafka
  "Start zk and kafka, invoke f, and then stop them"
  [zk-overrides kafka-overrides f]
  (let [ctx (start-standalone! zk-overrides kafka-overrides)]
    (try
      (f)
      (finally
        (stop-standalone! ctx)))))
