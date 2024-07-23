(ns ketu.spec
  (:require [clojure.set]
            [clojure.spec.alpha :as s]
            [clojure.string]
            [expound.alpha :as expound]
            [clojure.core.async.impl.protocols])
  (:import (java.util.regex Pattern)
           (org.apache.kafka.clients.producer Callback)
           (org.apache.kafka.common.serialization Deserializer Serializer)))

(s/def ::non-blank-string (s/and string? (complement clojure.string/blank?)))
(s/def ::->keyword (s/conformer #(or (some-> % keyword) ::s/invalid)))

(s/def :ketu/name ::non-blank-string)
(s/def :ketu/brokers ::non-blank-string)
(s/def :ketu/topic ::non-blank-string)
(s/def :ketu.apache.consumer/auto-offset-reset #{"earliest" "latest" "none"})
(s/def :ketu.apache.producer/compression-type #{"none" "gzip" "snappy" "lz4" "zstd"})

(s/def :ketu.source/topic :ketu/topic)
(s/def :ketu.source/topic-list (s/coll-of ::non-blank-string))
(s/def :ketu.source/topic-pattern #(instance? Pattern %))
(s/def :ketu.source/group-id ::non-blank-string)
(s/def :ketu.source/poll-timeout-ms pos-int?)
(s/def :ketu.source/done-putting-timeout-ms pos-int?)
(s/def :ketu.source/consumer-close-timeout-ms pos-int?)
(s/def :ketu.source/consumer-thread-timeout-ms nat-int?)
(s/def :ketu.source/close-out-chan? boolean?)
(s/def :ketu.source/close-consumer? boolean?)
(s/def :ketu.source/create-rebalance-listener-obj fn?)
(s/def :ketu.source/consumer-decorator fn?)
(s/def :ketu.source.assign/topic :ketu/topic)
(s/def :ketu.source.assign/partition-nums (s/coll-of nat-int?))
(s/def :ketu.source/assign-single-topic-partitions
  (s/keys :req [:ketu.source.assign/topic
                :ketu.source.assign/partition-nums]))

(s/def :ketu.source/shape
  (s/nonconforming
    (s/or :preset (s/and ::->keyword #{:value :key-value-vector :key-value-map :map})
          :schema (s/cat :type (s/and ::->keyword #{:vector :map})
                         :fields (s/+ (s/and ::->keyword
                                             #{:key :value :topic :partition :offset :timestamp :headers
                                               :timestamp-type :leader-epoch
                                               :serialized-key-size :serialized-value-size})))
          :custom (s/cat :type (s/and ::->keyword qualified-keyword?)
                         :fields (s/* any?))
          :fn fn?)))

(s/def :ketu.sink/shape
  (s/nonconforming
    (s/or :preset (s/and ::->keyword #{:value :key-value-vector :key-value-map})
          :schema (s/cat :type (s/and ::->keyword #{:vector :map})
                         :fields (s/+ (s/and ::->keyword
                                             (s/or :known #{:key :value :topic :partition :timestamp :headers}
                                                   :ignored #(-> % name (clojure.string/starts-with? "_"))))))
          :custom (s/cat :type (s/and ::->keyword qualified-keyword?)
                         :fields (s/* any?))
          :fn fn?)))

(s/def :ketu.source/value-type
  (s/nonconforming
    (s/or :preset (s/and ::->keyword #{:byte-array :string})
          :object #(instance? Deserializer %)
          :class class?
          :class-name string?)))

(s/def :ketu/source-opts
  (s/keys :req [:ketu/name
                (or :ketu.source/topic
                    :ketu.source/topic-list
                    :ketu.source/topic-pattern
                    :ketu.source/assign-single-topic-partitions)]
          :opt [:ketu.source/shape
                :ketu.source/poll-timeout-ms
                :ketu.source/done-putting-timeout-ms
                :ketu.source/consumer-close-timeout-ms
                :ketu.source/consumer-thread-timeout-ms
                :ketu.source/close-out-chan?
                :ketu.source/close-consumer?
                :ketu.source/consumer-commands-chan
                :ketu.source/consumer-interceptors]))

(s/def :ketu.apache.producer/config map?)
(s/def :ketu.sink/sender-threads-num pos-int?)
(s/def :ketu.sink/sender-threads-timeout-ms nat-int?)
(s/def :ketu.sink/close-producer? boolean?)
(s/def :ketu.sink/producer-close-timeout-ms nat-int?)
(s/def :ketu.sink/callback fn?)
(s/def :ketu.sink/callback-obj #(instance? Callback %))
(s/def :ketu.sink/create-callback fn?)
(s/def :ketu.sink/create-callback-obj fn?)

(s/def :ketu.sink/value-type
  (s/nonconforming
    (s/or :preset (s/and ::->keyword #{:byte-array :string})
          :object #(instance? Serializer %)
          :class class?
          :class-name string?)))

(s/def :ketu/sink-opts
  (s/keys :req [:ketu/name]
          :opt [:ketu/topic
                :ketu.sink/shape
                :ketu.sink/sender-threads-num
                :ketu.sink/sender-threads-timeout-ms
                :ketu.sink/close-producer?
                :ketu.sink/producer-close-timeout-ms]))

;; Coercing keys to canonical form

(defn unnamespace [k] (keyword (name k)))

(defn ->unnamespaced-kmap [ks]
  (into {} (map (fn [k] [(unnamespace k) k])) ks))

(def common-kmap
  {:name :ketu/name})

(def consumer-kmap
  {:shape :ketu.source/shape
   :key-type :ketu.source/key-type
   :value-type :ketu.source/value-type
   :topic :ketu.source/topic
   :topic-list :ketu.source/topic-list
   :topic-pattern :ketu.source/topic-pattern
   :internal-config :ketu.apache.consumer/config})

(def producer-kmap
  {:topic :ketu/topic
   :shape :ketu.sink/shape
   :key-type :ketu.sink/key-type
   :value-type :ketu.sink/value-type
   :workers :ketu.sink/sender-threads-num
   :internal-config :ketu.apache.producer/config})

; truncate namespace
(def unnamespaced-kmap
  (->unnamespaced-kmap
    [:ketu/brokers
     :ketu.source/group-id
     :ketu.source/create-rebalance-listener-obj
     :ketu.sink/create-callback
     :ketu.apache.consumer/auto-offset-reset
     :ketu.apache.producer/compression-type]))

(defn rename-source-keys [m]
  (clojure.set/rename-keys m (merge unnamespaced-kmap common-kmap consumer-kmap)))

(defn rename-sink-keys [m]
  (clojure.set/rename-keys m (merge unnamespaced-kmap common-kmap producer-kmap)))

(s/def :ketu/public-source-opts
  (s/and (s/conformer rename-source-keys)
         :ketu/source-opts))

(s/def :ketu/public-sink-opts
  (s/and (s/conformer rename-sink-keys)
         :ketu/sink-opts))

(defn assert-and-conform [spec x]
  (when-not (s/valid? spec x)
    (throw (ex-info (expound/expound-str spec x {:print-specs? false}) {::failure :assertion-failed})))
  (s/conform spec x))
