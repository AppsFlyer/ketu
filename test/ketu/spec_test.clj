(ns ketu.spec-test
  (:require [clojure.test :refer [are deftest testing is]]
            [clojure.reflect]
            [clojure.spec.alpha :as s]
            [clojure.string]
            [ketu.spec]
            [clojure.test.check :as tc]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.generators :as gen])
  (:import (org.apache.kafka.clients.consumer ConsumerRecord)
           (org.apache.kafka.clients.producer ProducerRecord Callback)
           (clojure.reflect Method)))

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
         (filter #(instance? Method %))
         (filter #(:public (:flags %)))
         (remove #(:static (:flags %)))
         (remove #(#{'toString 'equals 'hashCode} (:name %)))
         (map :name)
         (map ->kebab))))

(deftest sink-opts-tests
  (testing "that passing only the required fields is valid"
    (tc/quick-check
      10
      (prop/for-all [name (gen/such-that (complement clojure.string/blank?) gen/string)]
        (let [sink-opts {:ketu/name name}]
          (is (s/valid? :ketu/sink-opts sink-opts))))))

  (testing "that passing all possible valid fields is valid"
    (tc/quick-check
      50
      (prop/for-all [name                      (gen/such-that (complement clojure.string/blank?) gen/string)
                     topic                     (gen/such-that (complement clojure.string/blank?) gen/string)
                     shape                     (gen/elements [:value
                                                              :key-value-vector
                                                              :key-value-map
                                                              [:vector :key :value :topic :partition :timestamp :headers]
                                                              [:map :key :value :topic :partition :timestamp :headers]])
                     sender-threads-num        (gen/large-integer* {:min 1})
                     sender-threads-timeout-ms (gen/large-integer* {:min 0})
                     close-producer?           gen/boolean
                     producer-close-timeout-ms (gen/large-integer* {:min 1})
                     callback                  (gen/elements [boolean? int? contains? neg?])
                     callback-obj              (gen/return (reify Callback
                                                             (onCompletion [_ _ _])))
                     create-callback           (gen/elements [boolean? int? contains? neg?])
                     create-callback-obj       (gen/elements [boolean? int? contains? neg?])]
        (let [sink-opts {:ketu/name                           name
                         :ketu/topic                          topic
                         :ketu.sink/shape                     shape
                         :ketu.sink/sender-threads-num        sender-threads-num
                         :ketu.sink/sender-threads-timeout-ms sender-threads-timeout-ms
                         :ketu.sink/close-producer?           close-producer?
                         :ketu.sink/producer-close-timeout-ms producer-close-timeout-ms
                         :ketu.sink/callback                  callback
                         :ketu.sink/callback-obj              callback-obj
                         :ketu.sink/create-callback           create-callback
                         :ketu.sink/create-callback-obj       create-callback-obj}]
          (is (s/valid? :ketu/sink-opts sink-opts))))))

  (testing "that passing valid required fields and invalid optional fields is invalid"
    (tc/quick-check
      50
      (prop/for-all [name                      (gen/such-that (complement clojure.string/blank?) gen/string)
                     topic                     gen/large-integer
                     shape                     gen/string
                     sender-threads-num        gen/string
                     sender-threads-timeout-ms gen/string
                     close-producer?           gen/string
                     producer-close-timeout-ms gen/string
                     callback                  gen/string
                     callback-obj              gen/string
                     create-callback           gen/string
                     create-callback-obj       gen/string]
        (let [sink-opts {:ketu/name                           name
                         :ketu/topic                          topic
                         :ketu.sink/shape                     shape
                         :ketu.sink/sender-threads-num        sender-threads-num
                         :ketu.sink/sender-threads-timeout-ms sender-threads-timeout-ms
                         :ketu.sink/close-producer?           close-producer?
                         :ketu.sink/producer-close-timeout-ms producer-close-timeout-ms
                         :ketu.sink/callback                  callback
                         :ketu.sink/callback-obj              callback-obj
                         :ketu.sink/create-callback           create-callback
                         :ketu.sink/create-callback-obj       create-callback-obj}]
          (is (not (s/valid? :ketu/sink-opts sink-opts)))))))

  (testing "that passing undocumented fields is valid"
    (tc/quick-check
      50
      (prop/for-all [name (gen/such-that (complement clojure.string/blank?) gen/string)
                     age  gen/large-integer]
        (let [sink-opts {:ketu/name name
                         :ketu/age  age}]
          (is (s/valid? :ketu/sink-opts sink-opts)))))))

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
