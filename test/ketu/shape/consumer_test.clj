(ns ketu.shape.consumer-test
  (:require [clojure.test :refer [are deftest is testing]]
            [ketu.shape.consumer :as shape])
  (:import (java.util Optional Collections)
           (org.apache.kafka.clients.consumer ConsumerRecord)
           (org.apache.kafka.common.header.internals RecordHeaders RecordHeader)
           (org.apache.kafka.common.record TimestampType)))

(deftest shape-fn
  (let [topic "topic"
        ctx {:ketu/topic topic}
        partition (int 0)
        offset (long 1)
        k "k"
        v "v"
        timestamp (long 2)
        timestamp-type (TimestampType/CREATE_TIME)
        ksize 4
        vsize 5
        headers (RecordHeaders. (Collections/singletonList (RecordHeader. "header-key" ^bytes (.getBytes "header-val"))))
        epoch (Optional/of (long 6))
        r (ConsumerRecord. topic partition offset timestamp timestamp-type ksize vsize k v headers epoch)]

    (testing "Convert ConsumerRecord to data"
      (are [ks data] (= data (let [runtime-ks ks
                                   ->data (shape/->data-fn ctx runtime-ks)]
                               (->data r)))
        :value v
        :key-value-vector [k v]
        :key-value-map {:key k :value v}
        :legacy/map {:key k :value v :topic topic :partition partition :offset offset}
        [:field :value] v
        [:field :key] k
        [:vector :key :value] [k v]
        [:vector :topic :value] [topic v]
        [:vector :value :topic :partition :offset] [v topic partition offset]
        [:vector :value :headers] [v headers]
        [:vector :timestampType] [timestamp-type]
        [:vector :timestamp-type] [timestamp-type]
        [:vector :timestamp :timestamp-type :serialized-key-size :serialized-value-size :leader-epoch] [timestamp timestamp-type ksize vsize epoch]
        [:map :key :value] {:key k :value v}
        [:map :timestamp :timestamp-type] {:timestamp timestamp :timestamp-type timestamp-type}))))

(defmethod shape/->data-fn ::multimethod-vector [_ctx [_ extra]]
  (fn [^ConsumerRecord r]
    [extra (.key r) (.value r)]))

(deftest custom-shape
  (let [topic "topic"
        ctx {:ketu/topic topic}
        partition (int 0)
        offset (long 1)
        k "k"
        v "v"
        consumer-record (ConsumerRecord. topic partition offset k v)]
    (testing "Multimethod vector with an extra constant"
      (is (= ["const" k v]
             (let [shape [::multimethod-vector "const"]
                   ->data (shape/->data-fn ctx shape)]
               (->data consumer-record)))))

    (testing "Registered vector with an extra constant"
      (shape/register! ::registered-vector
                       (fn [_ctx [_ extra]]
                         (fn [^ConsumerRecord r]
                           [extra (.key r) (.value r)])))
      (is (= ["reg" k v]
             (let [shape [::registered-vector "reg"]
                   ->data (shape/->data-fn ctx shape)]
               (->data consumer-record)))))))
