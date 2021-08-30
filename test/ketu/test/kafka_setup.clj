(ns ketu.test.kafka-setup
  (:require [clj-test-containers.core :as tc])
  (:import (org.testcontainers.containers KafkaContainer)
           (org.testcontainers.utility DockerImageName)))

(def ^:private ^:dynamic *kafka-container* nil)

(defn get-bootstrap-servers
  ([]
   (get-bootstrap-servers *kafka-container*))
  ([kafka-container]
   (.getBootstrapServers ^KafkaContainer (:container kafka-container))))

(defn start-container []
  (-> {:container     (KafkaContainer. (DockerImageName/parse "confluentinc/cp-kafka:5.5.3"))
       :exposed-ports [(KafkaContainer/KAFKA_PORT)]}
      tc/init
      tc/start!))

(defn- stop-container! [container]
  (tc/stop! container))

(defn with-kafka-container [test-fn]
  (binding [*kafka-container* (start-container)]
    (try
      (test-fn)
      (finally
        (stop-container! *kafka-container*)))))
