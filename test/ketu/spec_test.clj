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
           (org.apache.kafka.clients.producer ProducerRecord)
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
  (testing "Passing only the required fields is valid"
    (tc/quick-check
      10
      (prop/for-all [name (gen/such-that (complement clojure.string/blank?) gen/string)]
                    (let [sink-opts {:ketu/name name}]
                      (is (s/valid? :ketu/sink-opts sink-opts))))))

  (testing "Passing all possible valid fields is valid"
    (tc/quick-check
      50
      (prop/for-all [name (gen/such-that (complement clojure.string/blank?) gen/string)
                     topic (gen/such-that (complement clojure.string/blank?) gen/string)
                     shape (gen/elements [:value
                                          :key-value-vector
                                          :key-value-map
                                          [:vector :key :value :topic :partition :timestamp :headers]
                                          [:map :key :value :topic :partition :timestamp :headers]])
                     sender-threads-num (gen/large-integer* {:min 1})
                     sender-threads-timeout-ms (gen/large-integer* {:min 0})
                     close-producer? gen/boolean
                     producer-close-timeout-ms (gen/large-integer* {:min 1})]
                    (let [sink-opts {:ketu/name name
                                     :ketu/topic topic
                                     :ketu.apache.client/security-protocol "SSL"
                                     :ketu.apache.client/ssl-endpoint-identification-algorithm ""
                                     :ketu.apache.client/ssl-keystore-location "keystore.p12"
                                     :ketu.apache.client/ssl-keystore-password "keystore-password"
                                     :ketu.apache.client/ssl-truststore-location "truststore.p12"
                                     :ketu.apache.client/ssl-truststore-password "truststore-password"
                                     :ketu.apache.client/ssl-key-password "key-password"
                                     :ketu.sink/shape shape
                                     :ketu.sink/sender-threads-num sender-threads-num
                                     :ketu.sink/sender-threads-timeout-ms sender-threads-timeout-ms
                                     :ketu.sink/close-producer? close-producer?
                                     :ketu.sink/producer-close-timeout-ms producer-close-timeout-ms}]
                      (is (s/valid? :ketu/sink-opts sink-opts))))))

  (testing "Passing valid required fields and invalid optional fields is invalid"
    (tc/quick-check
      50
      (prop/for-all [name (gen/such-that (complement clojure.string/blank?) gen/string)
                     topic (gen/one-of [(gen/return "") gen/large-integer])
                     shape gen/string
                     sender-threads-num (gen/one-of [gen/string (gen/large-integer* {:max -1})])
                     sender-threads-timeout-ms (gen/one-of [gen/string (gen/large-integer* {:max -1})])
                     close-producer? (gen/such-that some? gen/any)
                     producer-close-timeout-ms gen/string]
                    (let [sink-opts {:ketu/name name
                                     :ketu/topic topic
                                     :ketu.sink/shape shape
                                     :ketu.sink/sender-threads-num sender-threads-num
                                     :ketu.sink/sender-threads-timeout-ms sender-threads-timeout-ms
                                     :ketu.sink/close-producer? close-producer?
                                     :ketu.sink/producer-close-timeout-ms producer-close-timeout-ms}]
                      (is (not (s/valid? :ketu/sink-opts sink-opts)))))))

  (testing "Making sure the spec is open, meaning it doesn't fail on undocumented fields"
    (tc/quick-check
      50
      (prop/for-all [name (gen/such-that (complement clojure.string/blank?) gen/string)
                     age gen/large-integer]
                    (let [sink-opts {:ketu/name name
                                     :ketu/age age}]
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
