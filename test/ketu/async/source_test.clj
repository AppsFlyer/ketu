(ns ketu.async.source-test
  (:require [clojure.test :refer [deftest testing is are]]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols]
            [ketu.test.log :as log]
            [ketu.test.util :as u]
            [ketu.clients.consumer :as consumer]
            [ketu.async.source :as source])
  (:import (clojure.lang ExceptionInfo)
           (org.apache.kafka.clients.consumer MockConsumer OffsetResetStrategy ConsumerRecord)
           (org.apache.kafka.common KafkaException)
           (org.apache.kafka.common.serialization Deserializer)))

(defn mock-consumer
  "Create a fake consumer. Optionally fakes a topic.
   You can mock records on that topic with `add-record`."
  (^MockConsumer []
   (MockConsumer. OffsetResetStrategy/EARLIEST))
  (^MockConsumer [topic]
   (let [partition (consumer/topic-partition topic 0)]
     (doto (MockConsumer. OffsetResetStrategy/EARLIEST)
       (consumer/subscribe-to-topic! topic)
       (.rebalance [(consumer/topic-partition topic 0)])
       (.updateBeginningOffsets {partition 0})
       (.seek partition 0)))))

(defn add-record [^MockConsumer consumer record]
  (.addRecord consumer record))

(defn channel-closed? [ch]
  (clojure.core.async.impl.protocols/closed? ch))

(deftest init
  (testing "Doesn't close channel because opts are invalid so we can't trust them and we fail early"
    (let [incomplete-opts {}
          ch (async/chan)]
      (is (thrown? ExceptionInfo (source/source ch incomplete-opts)))
      (is (not (channel-closed? ch)))))

  (testing "Doesn't close channel even when configured to do so in invalid opts"
    (let [incomplete-opts {:ketu.source/close-out-chan? true}
          ch (async/chan)]
      (is (thrown? ExceptionInfo (source/source ch incomplete-opts)))
      (is (not (channel-closed? ch)))))

  (testing "Closing channel by default on error creating consumer"
    (let [no-brokers-opts {:name "name"
                           :topic "topic"}
          ch (async/chan)]
      (is (thrown? KafkaException (source/source ch no-brokers-opts)))
      (is (channel-closed? ch))))

  (testing "Not closing channel on error creating consumer when so configured"
    (let [no-brokers-opts {:name "name"
                           :topic "topic"
                           :ketu.source/close-out-chan? false}
          ch (async/chan)]
      (is (thrown? KafkaException (source/source ch no-brokers-opts)))
      (is (not (channel-closed? ch)))))

  (testing "Closing consumer and closing channel by default on error creating consumer-source"
    (let [closed-consumer? (atom false)
          opts {:name "name"
                :topic "topic"
                :brokers "localhost:9999"
                :value-type (reify Deserializer
                              (close [_]
                                (reset! closed-consumer? true)))}
          thrower (fn [_ _ _] (throw (ex-info "test" {:test true})))
          ch (async/chan)]
      (is (thrown? Exception (with-redefs [ketu.async.source/source-existing-consumer thrower]
                               (source/source ch opts))))
      (is @closed-consumer?)
      (is (channel-closed? ch))))

  (testing "Closing consumer but not channel on error creating consumer-source when so configured"
                                        ; We create the consumer internally so there's no point keeping it alive.
    (let [closed-consumer? (atom false)
          opts {:name "name"
                :topic "topic"
                :brokers "localhost:9999"
                :ketu.source/close-out-chan? false
                :value-type (reify Deserializer
                              (close [_]
                                (reset! closed-consumer? true)))}
          thrower (fn [_ _ _] (throw (ex-info "test" {:test true})))
          ch (async/chan)]
      (is (thrown? Exception (with-redefs [ketu.async.source/source-existing-consumer thrower]
                               (source/source ch opts))))
      (is @closed-consumer?)
      (is (not (channel-closed? ch))))))

(deftest shape
  (testing "Put ConsumerRecord objects by default"
    (let [record (ConsumerRecord. "topic" 0 0 "k" "v")
          consumer (doto (mock-consumer "topic")
                     (add-record record))
          ch (async/chan)
          source (source/source ch {:name "test"
                                    :topic "test-topic"
                                    :ketu.source/consumer-supplier (constantly consumer)})]
      (is (= record (u/try-take! ch)))
      (source/stop! source))))

(deftest basic-logs
  (testing "Just start and stop the source"
    (log/with-test-appender
      (log/ns-logger 'ketu.async.source)
      (fn [log-ctx]
        (let [consumer (mock-consumer)
              ch (async/chan)
              source (source/source ch {:name "test"
                                        :topic "test-topic"
                                        :ketu.source/consumer-supplier (constantly consumer)})]
          (source/stop! source)
          (is (= [[:info "[source=test] Start consumer thread"]
                  [:info "[source=test] Done consuming"]
                  [:info "[source=test] Close out channel"]
                  [:info "[source=test] Close consumer"]
                  [:info "[source=test] Exit consumer thread"]]
                 (log/events log-ctx))))))))

(deftest unexpected-wakeup-logs
  (testing "Throw unexpected WakeupException on first poll"
    (log/with-test-appender
      (log/ns-logger 'ketu.async.source)
      (fn [log-ctx]
        (let [consumer (doto (mock-consumer)
                         (consumer/wakeup!))
              ch (async/chan)
              source (source/source ch {:name "test"
                                        :topic "test-topic"
                                        :ketu.source/consumer-supplier (constantly consumer)})]
          (u/try-take! (source/done-chan source))
          (is (= [[:info "[source=test] Start consumer thread"]
                  [:error "[source=test] Unexpected consumer wakeup"]
                  [:info "[source=test] Done consuming"]
                  [:info "[source=test] Close out channel"]
                  [:info "[source=test] Close consumer"]
                  [:info "[source=test] Exit consumer thread"]]
                 (log/events log-ctx))))))))

(deftest unrecoverable-exception-logs
  (testing "Throw unrecoverable exception on first poll"
    (log/with-test-appender
      (log/ns-logger 'ketu.async.source)
      (fn [log-ctx]
        (let [consumer (doto (mock-consumer)
                         (.setPollException (KafkaException. "test exception")))
              ch (async/chan)
              source (source/source ch {:name "test"
                                        :topic "test-topic"
                                        :ketu.source/consumer-supplier (constantly consumer)})]
          (u/try-take! (source/done-chan source))
          (is (= [[:info "[source=test] Start consumer thread"]
                  [:error "[source=test] Unrecoverable consumer error"]
                  [:info "[source=test] Done consuming"]
                  [:info "[source=test] Close out channel"]
                  [:info "[source=test] Close consumer"]
                  [:info "[source=test] Exit consumer thread"]]
                 (log/events log-ctx))))))))


(deftest source-opts-test
  (testing "sanity"
    (are [config expected] (= expected (source/parse-opts config))
      {:topic "topic" :name "name"} {:ketu.source/consumer-supplier ketu.async.source/default-consumer-supplier
                                     :ketu.source/consumer-close-timeout-ms 60000
                                     :ketu.apache.consumer/config {"key.deserializer"
                                                                   "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                                                                   "value.deserializer"
                                                                   "org.apache.kafka.common.serialization.ByteArrayDeserializer"}
                                     :ketu.source/topic "topic"
                                     :ketu.source/consumer-thread-timeout-ms 60000
                                     :ketu.source/close-out-chan? true
                                     :ketu.source/close-consumer? true
                                     :ketu.source/poll-timeout-ms 100
                                     :ketu.source/value-type :byte-array
                                     :ketu.source/done-putting-timeout-ms 60000
                                     :ketu/name "name"
                                     :ketu.source/key-type :byte-array}
      )
    )
  (testing "ssl"
    (are [config expected] (= expected (source/parse-opts config))
      {:topic "topic"
       :name "name"
       :security-protocol "SSL"
       :group-id "group-id"
       :ssl-endpoint-identification-algorithm ""
       :ssl-key-password "key-password"
       :ssl-keystore-location "keystore-location"
       :ssl-keystore-password "keystore-password"
       :ssl-truststore-location "truststore-location"
       :ssl-truststore-password "truststore-password"} {:ketu.source/consumer-supplier ketu.async.source/default-consumer-supplier
                                                        :ketu.source/consumer-close-timeout-ms 60000
                                                        :ketu.apache.consumer/config {"key.deserializer"
                                                                                      "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                                                                                      "value.deserializer"
                                                                                      "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                                                                                      "group.id" "group-id"
                                                                                      "ssl.endpoint.identification.algorithm" ""
                                                                                      "security.protocol" "SSL"
                                                                                      "ssl.key.password" "key-password"
                                                                                      "ssl.keystore.location" "keystore-location"
                                                                                      "ssl.keystore.password" "keystore-password"
                                                                                      "ssl.truststore.location" "truststore-location"
                                                                                      "ssl.truststore.password" "truststore-password"}
                                                        :ketu.source/topic "topic"
                                                        :ketu.source/consumer-thread-timeout-ms 60000
                                                        :ketu.source/close-out-chan? true
                                                        :ketu.source/close-consumer? true
                                                        :ketu.source/poll-timeout-ms 100
                                                        :ketu.source/value-type :byte-array
                                                        :ketu.source/done-putting-timeout-ms 60000
                                                        :ketu/name "name"
                                                        :ketu.apache.client/security-protocol "SSL"
                                                        :ketu.apache.client/ssl-endpoint-identification-algorithm ""
                                                        :ketu.apache.client/ssl-key-password "key-password"
                                                        :ketu.apache.client/ssl-keystore-location "keystore-location"
                                                        :ketu.apache.client/ssl-keystore-password "keystore-password"
                                                        :ketu.apache.client/ssl-truststore-location "truststore-location"
                                                        :ketu.apache.client/ssl-truststore-password "truststore-password"
                                                        :ketu.source/group-id "group-id"
                                                        :ketu.source/key-type :byte-array})))
