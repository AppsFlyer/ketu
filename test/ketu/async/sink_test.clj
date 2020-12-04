(ns ketu.async.sink-test
  (:require [clojure.test :refer [deftest testing is]]
            [spy.core :as spy]
            [spy.assert]
            [clojure.core.async :as async]
            [ketu.async.sink :as sink]
            [ketu.clients.producer :as producer]
            [ketu.shape.producer]
            [ketu.test.log :as log]
            [ketu.test.util :as u])
  (:import (clojure.lang ExceptionInfo)
           (org.apache.kafka.clients.producer MockProducer RecordMetadata Callback)
           (org.apache.kafka.common.errors TimeoutException)
           (org.apache.kafka.common.serialization Serializer)))

(defn- mock-producer
  ^MockProducer
  []
  (let [auto-complete true
        serializer (producer/string-serializer)]
    (MockProducer. auto-complete serializer serializer)))

(deftest init
  (testing "Throw when failed creating a producer"
    (let [throwing-supplier (fn [_] (throw (ex-info "test" {:test true})))
          opts {:name "name"
                :ketu.sink/producer-supplier throwing-supplier}
          ch (async/chan)]
      (is (thrown-with-msg? ExceptionInfo #"test"
                            (sink/sink ch opts)))))

  (testing "Close producer by default when failed creating"
    (let [closed? (atom false)
          ;; missing topic so creating the sink will fail after creating the producer.
          opts {:name "name"
                :brokers "localhost:9999"
                :shape :value
                :value-type (reify Serializer
                              (close [_]
                                (reset! closed? true)))}
          ch (async/chan)]
      (is (thrown-with-msg? Exception #"Missing topic"
                            (sink/sink ch opts)))
      (is @closed?)))

  (testing "Don't close producer when failed creating sink if not configured to close"
    (let [closed? (atom false)
          ;; missing topic so creating the sink will fail after creating the producer.
          opts {:ketu.sink/close-producer? false
                :name "name"
                :brokers "localhost:9999"
                :shape :value
                :value-type (reify Serializer
                              (close [_]
                                (reset! closed? true)))}
          ch (async/chan)]
      (is (thrown-with-msg? Exception #"Missing topic"
                            (sink/sink ch opts)))
      (is (not @closed?)))))

(deftest shapes
  (testing "Custom producer shape"
    (let [producer (mock-producer)
          _ (ketu.shape.producer/register! ::cake (fn [_ [_ mold]]
                                                    (fn [v] (producer/record mold v))))
          opts {:name "name"
                :brokers "localhost:9999"
                :shape [::cake "bundt"]
                :ketu.sink/producer-supplier (constantly producer)}
          ch (async/chan 10)
          sink (sink/sink ch opts)]
      (async/>!! ch "v")
      (async/close! ch)
      (sink/stop! sink)
      (is (= [(producer/record "bundt" "v")] (.history producer))))))

(deftest callbacks
  (testing "Resolve a promise when its associated message is sent"
    (let [producer (mock-producer)
          ch (async/chan)
          ;; Transform each channel item from vector of [value resolve] to ProducerRecord, ignoring resolve
          ->record (fn [item]
                     (let [[value _] item]
                       (producer/record "test-topic" value)))
          ;; A callback factory, creating callbacks that call the resolve function of each input item.
          ->callback (spy/spy
                       (fn [item]
                         (let [[value resolve] item]
                           (fn [^RecordMetadata result _error]
                             (resolve value (.offset result))))))
          sink (sink/sink ch {:name "name"
                              :shape ->record
                              :create-callback ->callback
                              :ketu.sink/producer-supplier (constantly producer)})
          resolve0 (spy/spy (fn [_ _]))
          resolve1 (spy/spy (fn [_ _]))]
      ;; Produce two messages associated with promises
      (async/>!! ch ["0" resolve0])
      (async/>!! ch ["1" resolve1])
      ;; Close the sink to make sure messages were sent
      (async/close! ch)
      (sink/stop! sink)
      ;; The callback creator was called once for each item, and the promises resolved
      (spy.assert/called-n-times? ->callback 2)
      (spy.assert/called-with? ->callback ["0" resolve0])
      (spy.assert/called-with? ->callback ["1" resolve1])
      (spy.assert/called-once-with? resolve0 "0" 0)
      (spy.assert/called-once-with? resolve1 "1" 1)))

  (testing "Resolve a promise when its associated message is sent, using Callback objects"
    (let [producer (mock-producer)
          ch (async/chan)
          ;; Transform each channel item from vector of [value resolve] to ProducerRecord, ignoring resolve.
          ->record (fn [item]
                     (let [[value _] item]
                       (producer/record "test-topic" value)))
          ;; A callback factory, creating Callback objects that call the resolve function of each input item.
          ->callback-obj (spy/spy
                           (fn [item]
                             (let [[value resolve] item]
                               (reify Callback
                                 (^void onCompletion [_ ^RecordMetadata result ^Exception _error]
                                   (resolve value (.offset result)))))))
          sink (sink/sink ch {:name "name"
                              :shape ->record
                              :ketu.sink/create-callback-obj ->callback-obj
                              :ketu.sink/producer-supplier (constantly producer)})
          resolve0 (spy/spy (fn [_ _]))
          resolve1 (spy/spy (fn [_ _]))]
      ;; Produce two messages associated with promises
      (async/>!! ch ["0" resolve0])
      (async/>!! ch ["1" resolve1])
      ;; Close the sink to make sure messages were sent
      (async/close! ch)
      (sink/stop! sink)
      ;; The callback creator was called once for each item, and the promises resolved
      (spy.assert/called-n-times? ->callback-obj 2)
      (spy.assert/called-with? ->callback-obj ["0" resolve0])
      (spy.assert/called-with? ->callback-obj ["1" resolve1])
      (spy.assert/called-once-with? resolve0 "0" 0)
      (spy.assert/called-once-with? resolve1 "1" 1)))

  (testing "One callback function for all messages"
    ;; The single callback function has no context and is only useful for counting success/error etc.
    (let [producer (mock-producer)
          ch (async/chan)
          accept-offset (spy/spy (fn [_offset]))
          callback (fn [^RecordMetadata result _] (accept-offset (.offset result)))
          sink (sink/sink ch {:name "name"
                              :ketu.sink/callback callback
                              :ketu.sink/producer-supplier (constantly producer)})]
      ;; Produce two messages associated with promises
      (async/>!! ch (producer/record "test-topic" "0"))
      (async/>!! ch (producer/record "test-topic" "1"))
      ;; Close the sink to make sure messages were sent
      (async/close! ch)
      (sink/stop! sink)
      ;; The callback was called once for each result
      (spy.assert/called-n-times? accept-offset 2)
      (spy.assert/called-with? accept-offset 0)
      (spy.assert/called-with? accept-offset 1)))

  (testing "One Callback object for all messages"
    (let [producer (mock-producer)
          ch (async/chan)
          accept-offset (spy/spy (fn [_offset]))
          callback-obj (producer/callback (fn [^RecordMetadata result _] (accept-offset (.offset result))))
          sink (sink/sink ch {:name "name"
                              :ketu.sink/callback-obj callback-obj
                              :ketu.sink/producer-supplier (constantly producer)})]
      ;; Produce two messages associated with promises
      (async/>!! ch (producer/record "test-topic" "0"))
      (async/>!! ch (producer/record "test-topic" "1"))
      ;; Close the sink to make sure messages were sent
      (async/close! ch)
      (sink/stop! sink)
      ;; The callback was called once for each result
      (spy.assert/called-n-times? accept-offset 2)
      (spy.assert/called-with? accept-offset 0)
      (spy.assert/called-with? accept-offset 1))))

(deftest basic-logs
  (testing "Just start and stop the sink"
    (log/with-test-appender
      (log/ns-logger 'ketu.async.sink)
      (fn [appender]
        (let [producer (mock-producer)
              ch (async/chan)
              sink (sink/sink ch {:name "test"
                                  :ketu.sink/producer-supplier (constantly producer)})]
          (async/close! ch)
          (sink/stop! sink)
          (is (= #{[:info "[sink=test] Start 1 worker(s)"]
                   [:info "[sink=test worker=0] Start"]
                   [:info "[sink=test auto-close] Start"]
                   [:info "[sink=test worker=0] Exit"]
                   [:info "[sink=test auto-close] All workers are done"]
                   [:info "[sink=test] Close producer"]}
                 (set (log/events appender)))))))))

(deftest timeout-logs
  (testing "Producer send timeout"
    (log/with-test-appender
      (log/ns-logger 'ketu.async.sink)
      (fn [appender]
        (let [producer (mock-producer)
              ch (async/chan)
              sink (sink/sink ch {:name "test"
                                  :ketu.sink/producer-supplier (constantly producer)})
              time-out! (fn ([& _] (throw (TimeoutException.))))]
          (with-redefs [producer/send! time-out!]
            (u/try-put! ch (producer/record "test-topic" "value"))
            (async/close! ch)
            (sink/stop! sink))
          (is (some #{[:error "[sink=test] Send timeout"]} (log/events appender))))))))

(deftest unrecoverable-exception-logs
  (testing "Producer send unrecoverable exception"
    (log/with-test-appender
      (log/ns-logger 'ketu.async.sink)
      (fn [appender]
        (let [producer (mock-producer)
              ch (async/chan)
              sink (sink/sink ch {:name "test"
                                  :ketu.sink/producer-supplier (constantly producer)})
              test-exception (Exception. "test exception")
              throw! (fn ([& _] (throw test-exception)))]
          (with-redefs [producer/send! throw!]
            (u/try-put! ch (producer/record "test-topic" "value"))
            (async/close! ch)
            (sink/stop! sink))
          (is (some #{[:error "[sink=test worker=0] Unrecoverable error"]} (log/events appender))))))))

(deftest auto-close-error-log
  (log/with-test-appender
    (log/ns-logger 'ketu.async.sink)
    (fn [appender]
      (let [producer (mock-producer)
            opts {:name "test"
                  :ketu.sink/producer-supplier (constantly producer)}
            ch (async/chan)
            sink (sink/sink ch opts)
            throw! (fn ([& _] (throw (Exception. "test exception"))))]
        (with-redefs [producer/close! throw!]
          (async/close! ch)
          (u/try-take! (sink/done-chan sink)))
        (is (some #{[:error "[sink=test auto-close] Error"]} (log/events appender)))))))
