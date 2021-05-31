(ns ketu.test.kafka-setup
  (:require [clj-test-containers.core :as tc])
  (:import (org.testcontainers.containers KafkaContainer)
           (org.testcontainers.utility DockerImageName)))

(def ^:private kafka-container (atom nil))
(def ^:private ^:const container-port (KafkaContainer/KAFKA_PORT))

(defn get-bootstrap-servers
  ([]
   (get-bootstrap-servers @kafka-container))
  ([container]
   (.getBootstrapServers ^KafkaContainer (:container container))))

(defn start-container []
  (-> {:container     (KafkaContainer. (DockerImageName/parse "confluentinc/cp-kafka:5.5.3"))
       :exposed-ports [container-port]}
      tc/init
      tc/start!))

(defn- stop-container! [container]
  (tc/stop! container))

(defn with-kafka-container [test-fn]
  (let [container (start-container)]
    (reset! kafka-container container)
    (test-fn)
    (stop-container! container)
    (reset! kafka-container nil)))
