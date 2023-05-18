(ns ketu.async.util
  (:require [clojure.set]
            [ketu.spec :as spec])
  (:import (org.apache.kafka.common.serialization Deserializer Serializer)))

(defn- preset-deserializer-class [value-type]
  (case value-type
    :string "org.apache.kafka.common.serialization.StringDeserializer"
    :byte-array "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    nil))

(defn- type-deserializer-class [type]
  (cond
    ;; A deserializer object is passed as constructor param instead of *.deserializer config.
    (instance? Deserializer type)
    nil

    ;; Try one of the presets
    (or (string? type) (keyword? type) (symbol? type))
    (-> type keyword preset-deserializer-class)

    ;; Class name (string) or Class
    (some? type)
    type))

(defn- set-deserializers
  "Sets key.deserializer and value.deserializer in internal-config.
  If the type option is a serializer object, we don't touch the internal-config.
  Otherwise we translate it to a serializer class."
  [internal-config opts]
  (let [final-key-class (when-not (get internal-config "key.deserializer")
                          (type-deserializer-class (:ketu.source/key-type opts)))
        final-value-class (when-not (get internal-config "value.deserializer")
                            (type-deserializer-class (:ketu.source/value-type opts)))]
    (cond-> internal-config
      final-key-class (assoc "key.deserializer" final-key-class)
      final-value-class (assoc "value.deserializer" final-value-class))))

(defn- preset-serializer-class [value-type]
  (case value-type
    :string "org.apache.kafka.common.serialization.StringSerializer"
    :byte-array "org.apache.kafka.common.serialization.ByteArraySerializer"
    nil))

(defn- type-serializer-class [type]
  (cond
    ;; A serializer object is passed as constructor param instead of *.serializer config.
    (instance? Serializer type)
    nil

    ;; Try one of the presets
    (or (string? type) (keyword? type) (symbol? type))
    (-> type keyword preset-serializer-class)

    ;; Class name (string) or Class
    (some? type)
    type))

(defn- set-serializers
  "Sets key.serializer and value.serializer in internal-config.
  If the type option is a serializer object, we don't touch the internal-config.
  Otherwise we translate it to a serializer class."
  [internal-config opts]
  (let [final-key-class (when-not (get internal-config "key.serializer")
                          (type-serializer-class (:ketu.sink/key-type opts)))
        final-value-class (when-not (get internal-config "value.serializer")
                            (type-serializer-class (:ketu.sink/value-type opts)))]
    (cond-> internal-config
      final-key-class (assoc "key.serializer" final-key-class)
      final-value-class (assoc "value.serializer" final-value-class))))

(defn set-ketu-to-apache-opts
  "Translates specific top-level opts to the internal java api config
  and merges to the original internal config (original values win)."
  [internal-config opts]
  (let [kmap {:ketu/brokers                                             "bootstrap.servers"
              :ketu.source/group-id                                     "group.id"
              :ketu.apache.consumer/auto-offset-reset                   "auto.offset.reset"
              :ketu.apache.producer/compression-type                    "compression.type"
              :ketu.apache.client/security-protocol                     "security.protocol"
              :ketu.apache.client/ssl-endpoint-identification-algorithm "ssl.endpoint.identification.algorithm"
              :ketu.apache.client/ssl-truststore-location               "ssl.truststore.location"
              :ketu.apache.client/ssl-truststore-password               "ssl.truststore.password"
              :ketu.apache.client/ssl-keystore-location                 "ssl.keystore.location"
              :ketu.apache.client/ssl-keystore-password                 "ssl.keystore.password"
              :ketu.apache.client/ssl-key-password                      "ssl.key.password"}]
    (-> opts
        (select-keys (keys kmap))
        (clojure.set/rename-keys kmap)
        (set-deserializers opts)
        (set-serializers opts)
        (merge internal-config))))


(defn- finalize-apache-config
  "Returns opts with final :ketu.apache.producer/config entry"
  [spec opts]
  (let [original-config (spec opts)
        config (set-ketu-to-apache-opts original-config opts)]
    (assoc opts spec config)))

(defn- finalize-opts [default-opts-fn config-spec opts]
  (-> (default-opts-fn)
      (merge opts)
      (->> (finalize-apache-config config-spec))))

(defn parse-opts [spec config-spec default-opts-fn opts]
  (->> opts
      (ketu.spec/assert-and-conform spec)
      (finalize-opts default-opts-fn config-spec)))
