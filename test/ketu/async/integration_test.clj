(ns ketu.async.integration-test
  (:require [clojure.test :refer [use-fixtures deftest is testing]]
            [spy.core :as spy]
            [spy.assert]
            [ketu.test.util :as u]
            [clojure.core.async :as async]
            [ketu.clients.consumer :as consumer]
            [ketu.clients.producer :as producer]
            [ketu.async.source :as source]
            [ketu.async.sink :as sink]
            [ketu.test.kafka-setup :as kafka-setup])
  (:import (java.time Duration)
           (org.apache.kafka.common PartitionInfo TopicPartition)
           (org.apache.kafka.clients.producer RecordMetadata)
           (org.apache.kafka.clients.consumer Consumer)
           (org.apache.kafka.clients.admin AdminClient NewTopic)
           (java.util.concurrent TimeUnit)))

(use-fixtures :each kafka-setup/with-kafka-container)

(defn- fill-topic [topic values]
  (let [;; Start a producer-sink with results chan
        producer-chan (async/chan 10)
        result-chan (async/chan 10)
        sink-opts {:topic topic
                   :shape :value
                   :name "test-sink"
                   :key-type :string
                   :value-type :string
                   :brokers (kafka-setup/get-bootstrap-servers)
                   :ketu.sink/callback-obj (producer/callback (fn [_ _] (async/>!! result-chan ::done)))}
        sink (sink/sink producer-chan sink-opts)]
    ;; Produce the values
    (run! #(u/try-put! producer-chan %) values)
    ;; Wait for messages to finish
    (dotimes [_ (count values)] (u/try-take! result-chan))
    ;; Close the producer-sink and channels as we don't need them after creating the test data.
    (async/close! producer-chan)
    (sink/stop! sink)))

(deftest end-to-end
  (let [consumer-chan (async/chan 10)
        clicks-consumer-opts {:name "clicks-consumer"
                              :brokers (kafka-setup/get-bootstrap-servers)
                              :topic "clicks"
                              :group-id "clicks-test-consumer"
                              :auto-offset-reset "earliest"
                              :key-type :string
                              ;; Also testing internal-config
                              :internal-config {"value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"}
                              :shape [:vector :key :value :offset]}
        source (source/source consumer-chan clicks-consumer-opts)
        clicks-producer-opts {:name "clicks-producer"
                              :brokers (kafka-setup/get-bootstrap-servers)
                              :topic "clicks"
                              :key-type :string
                              :internal-config {"value.serializer" "org.apache.kafka.common.serialization.StringSerializer"}
                              :shape [:vector :key :value]}
        producer-chan (async/chan 10)
        sink (sink/sink producer-chan clicks-producer-opts)]
    (u/try-put! producer-chan ["k" "v"])
    (is (= ["k" "v" 0] (u/try-take! consumer-chan)))
    (source/stop! source)
    (async/close! producer-chan)
    (sink/stop! sink)))

(deftest shapes-and-topic-list
  (let [;; Producer-sink that will send data to clicks and matches topics
        multi-producer-opts {:name "multi-producer"
                             :brokers (kafka-setup/get-bootstrap-servers)
                             :key-type :string
                             :value-type :string
                             :shape [:vector :topic :key :value]}
        producer-chan (async/chan 10)
        sink (sink/sink producer-chan multi-producer-opts)
        ;; Consumer-source that will read from clicks and matches
        multi-consumer-opts {:name "multi-consumer"
                             :brokers (kafka-setup/get-bootstrap-servers)
                             :topic-list ["clicks" "matches"]
                             :group-id "multi-group"
                             :auto-offset-reset "earliest"
                             :key-type :string
                             :value-type :string
                             :shape [:vector :topic :key :value]}
        consumer-chan (async/chan 10)
        source (source/source consumer-chan multi-consumer-opts)]
    ;; Put one message per topic
    (u/try-put! producer-chan ["clicks" "click-k" "click-v"])
    (u/try-put! producer-chan ["matches" "match-k" "match-v"])
    ;; Take two messages that should be the same as those we produced
    (is (= #{["clicks" "click-k" "click-v"]
             ["matches" "match-k" "match-v"]}
           (set (repeatedly 2 #(u/try-take! consumer-chan)))))
    ;; Close consumer-source
    (source/stop! source)
    ;; Close producer-sink's channel which will auto-close the producer
    (async/close! producer-chan)
    ;; Just in case, manually close producer-sink
    (sink/stop! sink)))

(deftest producer-callback
  (testing "Resolve a promise when its associated message is sent"
    (let [clicks-producer-opts {:name "clicks-producer"
                                :brokers (kafka-setup/get-bootstrap-servers)
                                :topic "clicks"
                                :key-type :string
                                :value-type :string
                                ;; Transform each channel item from vector of [value resolve] to ProducerRecord, ignoring resolve
                                :shape [:vector :value :_resolve]
                                ;; A callback factory, creating callbacks that call the resolve function of each input item.
                                :create-callback (fn [[value resolve]]
                                                   (fn [^RecordMetadata result _error]
                                                     (resolve value (.offset result))))}
          resolve0 (spy/spy (fn [_ _]))
          resolve1 (spy/spy (fn [_ _]))
          producer-chan (async/chan 10)
          sink (sink/sink producer-chan clicks-producer-opts)]
      ;; Produce two messages associated with promises
      (u/try-put! producer-chan ["0" resolve0])
      (u/try-put! producer-chan ["1" resolve1])
      ;; Close the sink to make sure messages were sent
      (async/close! producer-chan)
      (sink/stop! sink)
      ;; The promises were resolved
      (spy.assert/called-once-with? resolve0 "0" 0)
      (spy.assert/called-once-with? resolve1 "1" 1))))

(deftest find-end-offset
  ;; Fill the topic with 3 messages
  (let [topic "offset-test-topic"
        _ (fill-topic topic ["0" "1" "2"])]

    (testing "End-offset directly with end-offsets function (recommended)"
      (let [;; Start a consumer without subscribing and without a group
            consumer (consumer/consumer {"bootstrap.servers" (kafka-setup/get-bootstrap-servers)
                                         "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                                         "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})
            ;; Get all the topic partitions info
            partitions-info (consumer/partitions-for consumer topic (Duration/ofMillis 5000))
            partition-ids (mapv (fn [^PartitionInfo pinfo] (.partition pinfo)) partitions-info)
            topic-partitions (consumer/topic-partitions topic partition-ids)
            ;; Get the end-offset of all the topic partitions
            java-end-offsets (consumer/end-offsets consumer topic-partitions (Duration/ofMillis 5000))
            end-offsets (into {} (map (fn [[k v]] [(str k) v])) java-end-offsets)
            ;; Close consumer
            _ (consumer/close! consumer 5000)]
        (is (= {"offset-test-topic-0" 3} end-offsets))))

    (testing "End-offset with assign and seek-to-end! function (not recommended)"
      (let [;; Start a consumer without subscribing and without a group
            consumer (consumer/consumer {"bootstrap.servers" (kafka-setup/get-bootstrap-servers)
                                         "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                                         "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})
            ;; Get all the topic partitions info
            partitions-info (consumer/partitions-for consumer topic (Duration/ofMillis 5000))
            partition-ids (mapv (fn [^PartitionInfo pinfo] (.partition pinfo)) partitions-info)
            topic-partitions (consumer/topic-partitions topic partition-ids)
            ;; Assign all partitions to consumer manually instead of subscribing which is slower.
            _ (consumer/assign! consumer topic-partitions)
            ;; Seek to end of all topic partitions
            _ (consumer/seek-to-end! consumer topic-partitions)
            ;; Get position for each partition
            positions (mapv #(consumer/position consumer %) topic-partitions)
            ;; Close consumer
            _ (consumer/close! consumer 5000)]
        (is (= [3] positions))))))

(deftest rebalance-listener
  (testing "Create ConsumerRebalanceListener object when source subscribes"
    (let [topic "test-topic"
          ;; Create test-topic with two partitions
          admin-client (AdminClient/create {"bootstrap.servers" (kafka-setup/get-bootstrap-servers)})
          _ (-> admin-client
                (.createTopics [(NewTopic. topic (int 2) (short 1))])
                (.all)
                (.get 5 TimeUnit/SECONDS))
          assignment-chan (async/chan (async/dropping-buffer 10))
          ->partitions (fn [tps] (mapv #(.partition ^TopicPartition %) tps))
          create-listener (fn [ctx]
                            (let [^Consumer consumer (:ketu.source/consumer ctx)
                                  assigned (fn [tps]
                                             (u/try-put! assignment-chan
                                                         {:assigned (->partitions tps)
                                                          :consumer-assignment (->partitions (.assignment consumer))}))
                                  revoked (fn [tps]
                                            (u/try-put! assignment-chan
                                                        {:revoked (->partitions tps)
                                                         :consumer-assignment (->partitions (.assignment consumer))}))]
                              (consumer/rebalance-listener assigned revoked)))
          opts {:name "test"
                :brokers (kafka-setup/get-bootstrap-servers)
                :topic topic
                :group-id "test-group"
                :key-type :string
                :value-type :string
                :create-rebalance-listener-obj create-listener}
          ch (async/chan 10)
          source (source/source ch opts)]
      ;; The source is polling in a background thread and will eventually trigger one initial assignment callback.
      (is (= {:assigned [0 1] :consumer-assignment [0 1]}
             (u/try-take! assignment-chan)))
      ;; Start another identical consumer to trigger a rebalance
      (let [source-rebalance (source/source ch opts)]
        ;; First, the original consumer partitions are revoked.
        (is (= {:revoked [0 1] :consumer-assignment [0 1]}
               (u/try-take! assignment-chan)))
        ;; Then, each consumer is assigned one partition
        (is (= #{{:assigned [0] :consumer-assignment [0]}
                 {:assigned [1] :consumer-assignment [1]}}
               (hash-set (u/try-take! assignment-chan) (u/try-take! assignment-chan))))
        (source/stop! source-rebalance)
        (source/stop! source)
        (.close ^AdminClient admin-client)))))
