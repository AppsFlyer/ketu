(ns ketu.clients.producer
  (:import [java.util.concurrent Future]
           [org.apache.kafka.clients.producer Callback KafkaProducer ProducerRecord RecordMetadata Producer]
           [org.apache.kafka.common.serialization Serializer ByteArraySerializer StringSerializer]
           (java.util Map)
           (java.time Duration)))

(defn byte-array-serializer ^ByteArraySerializer [] (ByteArraySerializer.))

(defn string-serializer ^StringSerializer [] (StringSerializer.))

(defn producer
  "The single param version assumes key and value serializer classes in config."
  (^KafkaProducer [^Map config]
   (KafkaProducer. config))
  (^KafkaProducer [^Map config ^Serializer key-serializer ^Serializer value-serializer]
   (KafkaProducer. config key-serializer value-serializer)))

(defn record
  "Creates a ProducerRecord.
  We support the basic overloads:
  [topic value] for value only,
  [topic key value] for partition by key,
  [topic partition timestamp key value headers] for manual partition/timestamp or headers. Pass nil if irrelevant."
  (^ProducerRecord [^String topic value]
   (ProducerRecord. topic value))
  (^ProducerRecord [^String topic key value]
   (ProducerRecord. topic key value))
  (^ProducerRecord [^String topic ^Integer partition ^Long timestamp key value ^Iterable headers]
   (ProducerRecord. topic partition timestamp key value headers)))

(defn callback ^Callback [f]
  (reify Callback
    (^void onCompletion [_ ^RecordMetadata record-metadata ^Exception exception]
      (f record-metadata exception)
      nil)))

(defn send!
  "For convenience, create producer, record and callback with the respective clojure functions in this namespace.
  Callback may be nil."
  (^Future [^Producer producer ^ProducerRecord record]
   (.send producer record))
  (^Future [^Producer producer ^ProducerRecord record ^Callback callback]
   (.send producer record callback)))

(defn close!
  [^Producer producer ^long timeout-ms]
  (.close producer (Duration/ofMillis timeout-ms)))

(defn flush!
  [^Producer producer]
  (.flush producer))
