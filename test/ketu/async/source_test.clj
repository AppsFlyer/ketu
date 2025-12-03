(ns ketu.async.source-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols]
            [clojure.string :as str]
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

(deftest poll-catch-fn
  (testing "Custom catch function is called, receives correct parameters, and can return empty collection"
    (let [received-opts      (atom nil)
          topic              "test-topic"
          partition          (consumer/topic-partition topic 0)
          poll-error-handler (fn [consumer opts]
                               (reset! received-opts opts)
                               (consumer/seek! consumer partition 1)
                               [])                          ; Return empty collection
          consumer           (doto (mock-consumer topic)
                               (.setPollException (KafkaException. "test exception")))
          ch                 (async/chan)
          opts               {:name                          "test"
                              :topic                         topic
                              :ketu.source/consumer-supplier (constantly consumer)
                              :ketu.source/poll-error-handler poll-error-handler
                              :ketu.source/close-out-chan? false
                              :custom-opt "custom-value"}
          source             (source/source ch opts)]
      (add-record consumer (ConsumerRecord. topic 0 0 "test-key" "test-value"))
      (Thread/sleep 100)
      (is (= "custom-value" (:custom-opt @received-opts)))
      (is (not (channel-closed? ch)))
      (let [timeout-ch (async/timeout 100)
            [item _] (async/alts!! [ch timeout-ch] :priority true)]
        (is (nil? item) "Catch function returns [], so no records should be put on channel"))
      (source/stop! source)))

  (testing "Default catch function handles faulty message gracefully and then processes healthy message"
    (let [orig-poll consumer/poll!]
      (with-redefs [ketu.clients.consumer/poll!
                    (fn [c t]
                      (let [records (orig-poll c t)]
                        (if (some #(or (= "faulty-key" (.key %))
                                       (= "faulty-value" (.value %)))
                                  records)
                          (do
                            ;; Reset position to 0 to simulate that we are stuck at the faulty record
                            ;; Since orig-poll advanced it, we must rewind for the test logic to be valid.
                            (consumer/seek! c (consumer/topic-partition "test-topic" 0) 0)
                            (throw (KafkaException. "Simulated corruption")))
                          records)))]
        (log/with-test-appender
          (log/ns-logger 'ketu.async.source)
          (fn [log-ctx]
            (let [consumer (mock-consumer "test-topic")
                  ch       (async/chan)
                  source   (source/source ch {:name                          "test"
                                              :topic                         "test-topic"
                                              :ketu.source/consumer-supplier (constantly consumer)
                                              :ketu.source/close-out-chan?   false})]
              (add-record consumer (ConsumerRecord. "test-topic" 0 0 "faulty-key" "faulty-value"))
              (Thread/sleep 100)
              (is (some #(and (= :error (first %))
                              (str/includes? (second %) "Caught poll exception"))
                        (log/events log-ctx)))
              (add-record consumer (ConsumerRecord. "test-topic" 0 1 "healthy-key" "healthy-value"))
              (Thread/sleep 100)
              (let [received-record (u/try-take! ch)]
                (is (= "healthy-key" (.key received-record)))
                (is (= "healthy-value" (.value received-record)))
                (is (= 1 (.offset received-record))))
              (source/stop! source))))))))