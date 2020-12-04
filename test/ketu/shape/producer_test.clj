(ns ketu.shape.producer-test
  (:require [clojure.test :refer [are deftest is testing]]
            [ketu.shape.producer :as shape])
  (:import (java.util Collections)
           (org.apache.kafka.clients.producer ProducerRecord)
           (org.apache.kafka.common.header.internals RecordHeaders RecordHeader)))

(deftest utils
  (testing "shapes that require topic"
    (are [shape] (is (shape/requires-topic? shape))
      :value
      :key-value-vector
      :key-value-map
      [:vector :key :value]))
  (testing "shapes that don't require topic"
    (are [shape] (is (not (shape/requires-topic? shape)))
      [:vector :topic :value]
      [:map :key :value :topic])))

(deftest shapes
  (let [topic "topic"
        ctx {:ketu/topic topic}
        topic-override "override"
        partition (int 0)
        offset (long 1)
        k "k"
        v "v"
        timestamp (long 2)
        headers (RecordHeaders. (Collections/singletonList (RecordHeader. "header-key" ^bytes (.getBytes "header-val"))))]
    (testing "converting data to ProducerRecord"
      (are [ks data record] (= record (let [runtime-ks ks
                                            ->record (shape/->record-fn ctx runtime-ks)]
                                        (->record data)))
        :value v (ProducerRecord. topic v)
        :key-value-vector [k v] (ProducerRecord. topic k v)
        :key-value-map {:key k :value v} (ProducerRecord. topic k v)
        [:vector :key :value] [k v] (ProducerRecord. topic k v)
        [:vector :key :_ignored :_ :value] [k 123 "extra" v] (ProducerRecord. topic k v)
        [:vector :topic :value] [topic-override v] (ProducerRecord. topic-override nil v)
        [:vector :value :topic :partition] [v topic-override partition offset] (ProducerRecord. topic-override partition nil v)
        [:vector :value :headers] [v headers] (ProducerRecord. topic nil nil nil v headers)
        [:vector :key :value :topic :partition :timestamp :headers] [k v topic-override partition timestamp headers] (ProducerRecord. topic-override partition timestamp k v headers)
        [:map :key :value] {:key k :value v} (ProducerRecord. topic k v)
        [:map :key :value :topic] {:key k :value v :topic topic-override} (ProducerRecord. topic-override k v)
        [:map :value :timestamp] {:value v :timestamp timestamp} (ProducerRecord. topic nil timestamp nil v nil)))))
