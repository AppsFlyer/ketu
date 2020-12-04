(ns ketu.shape.spec-test
  (:require [clojure.test :refer [are deftest testing]]
            [clojure.reflect]
            [clojure.spec.alpha :as s]
            [clojure.string]
            [ketu.spec])
  (:import (org.apache.kafka.clients.consumer ConsumerRecord)
           (org.apache.kafka.clients.producer ProducerRecord)))

(defn ->kebab [s]
  (clojure.string/replace s
                          #"([a-z])([A-Z])"
                          (fn [[_ x y]]
                            (str x "-" (clojure.string/lower-case y)))))

(defn record-fields [of]
  (let [of-class ({:consumer ConsumerRecord :producer ProducerRecord} of)]
    (->> of-class
         clojure.reflect/type-reflect
         :members
         (filter #(instance? clojure.reflect.Method %))
         (filter #(:public (:flags %)))
         (remove #(:static (:flags %)))
         (remove #(#{'toString 'equals 'hashCode} (:name %)))
         (map :name)
         (map ->kebab))))

(deftest spec
  (testing "Documented valid source shapes"
    (are [shape] (s/valid? :ketu.source/shape shape)
      :value
      :key-value-vector
      :key-value-map
      [:vector :key :value]
      [:map :key :value]
      (into [:vector] (map keyword (record-fields :consumer)))
      (into [:map] (map keyword (record-fields :consumer)))))

  (testing "Documented valid sink shapes"
    (are [shape] (s/valid? :ketu.sink/shape shape)
      :value
      :key-value-vector
      :key-value-map
      [:vector :key :value]
      [:map :key :value]
      [:vector :_ignored :_ :value]
      [:map :_ignored :_ :value]
      (into [:vector] (map keyword (record-fields :producer)))
      (into [:map] (map keyword (record-fields :producer)))))

  (testing "Custom shapes"
    (are [shape] (s/valid? :ketu.sink/shape shape)
      [::cake :bundt]
      [::cake "bundt"])))
