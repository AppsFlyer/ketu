(ns ketu.clients.consumer
  (:import (java.time Duration)
           (java.util Map Collection Collections)
           (org.apache.kafka.clients.consumer KafkaConsumer OffsetCommitCallback ConsumerRecords Consumer ConsumerRebalanceListener)
           (org.apache.kafka.common.serialization Deserializer ByteArrayDeserializer StringDeserializer)
           (java.util.regex Pattern)
           (org.apache.kafka.common TopicPartition)))

;; Deserializers

(defn byte-array-deserializer ^ByteArrayDeserializer [] (ByteArrayDeserializer.))

(defn string-deserializer ^StringDeserializer [] (StringDeserializer.))

;; The consumer itself

(defn consumer
  "Construct the java KafkaConsumer."
  (^KafkaConsumer [^Map config]
   (KafkaConsumer. config))
  (^KafkaConsumer [^Map config ^Deserializer key-deserializer ^Deserializer value-deserializer]
   (KafkaConsumer. config key-deserializer value-deserializer)))

;; Useful objects that are used as consumer functions parameters

(defn topic-partition
  "Create a TopicPartition object, to be used with e.g `seek!`"
  ^TopicPartition
  [^String topic partition]
  (TopicPartition. topic (int partition)))

(defn topic-partitions
  "Create a TopicPartition collection for a single topic, to be used with e.g `assign!`."
  ^Collection
  [^String topic partition-numbers]
  (mapv #(topic-partition topic %) partition-numbers))

(defn commit-callback
  "Construct a callback object for `commit!`"
  ^OffsetCommitCallback
  [f]
  (reify OffsetCommitCallback
    (^void onComplete [_ ^Map offsets ^Exception e]
      (f offsets e)
      nil)))

(defn rebalance-listener
  "Create a ConsumerRebalanceListener.
  Provide either 2 functions for assigned and revoked, or 3 for assigned, revoked and lost events.
  Each of assigned, revoked and lost functions receive a single parameter of type Collection<TopicPartition>."
  ^ConsumerRebalanceListener
  ([assigned revoked]
   (reify
     ConsumerRebalanceListener
     (onPartitionsAssigned [_ partitions]
       (assigned partitions))
     (onPartitionsRevoked [_ partitions]
       (revoked partitions))))
  ([assigned revoked lost]
   (reify
     ConsumerRebalanceListener
     (onPartitionsAssigned [_ partitions]
       (assigned partitions))
     (onPartitionsRevoked [_ partitions]
       (revoked partitions))
     (onPartitionsLost [_ partitions]
       (lost partitions)))))

;; Getters

(defn assignment
  "Get the set of partitions currently assigned to this consumer."
  [^Consumer consumer]
  (.assignment consumer))

(defn beginning-offsets
  "Get the first offset for the given partitions."
  ([^Consumer consumer partitions]
   (.beginningOffsets consumer partitions))
  ([^Consumer consumer partitions ^Duration timeout]
   (.beginningOffsets consumer partitions timeout)))

(defn end-offsets
  "Get the end offsets for the given partitions."
  ([^Consumer consumer partitions]
   (.endOffsets consumer partitions))
  ([^Consumer consumer partitions ^Duration timeout]
   (.endOffsets consumer partitions timeout)))

(defn partitions-for
  "Get metadata about the partitions for a given topic."
  ([^Consumer consumer topic]
   (.partitionsFor consumer topic))
  ([^Consumer consumer topic ^Duration timeout]
   (.partitionsFor consumer topic timeout)))

(defn position
  "Get the offset of the next record that will be fetched (if a record with that offset exists)."
  ([^Consumer consumer ^TopicPartition partition]
   (.position consumer partition))
  ([^Consumer consumer ^TopicPartition partition ^Duration timeout]
   (.position consumer partition timeout)))

(defn assign!
  "Manually assign specific partitions to the consumer without group coordination."
  [^Consumer consumer ^Collection partitions]
  (.assign consumer partitions))

(defn seek!
  "Overrides the fetch offsets that the consumer will use on the next poll."
  [^Consumer consumer ^TopicPartition partition offset]
  (.seek consumer partition (long offset)))

(defn seek-to-beginning!
  "Seek to the first offset for each of the given partitions (`Collection<TopicPartition>`)"
  [^Consumer consumer partitions]
  (.seekToBeginning consumer partitions))

(defn seek-to-end!
  "Seek to the last offset for each of the given partitions (`Collection<TopicPartition>`)."
  [^Consumer consumer partitions]
  (.seekToEnd consumer partitions))

(defn subscribe-to-topic!
  ([^Consumer consumer ^String topic]
   (.subscribe consumer (Collections/singletonList topic)))
  ([^Consumer consumer ^String topic ^ConsumerRebalanceListener rebalance-listener]
   (.subscribe consumer (Collections/singletonList topic) rebalance-listener)))

(defn subscribe-to-list!
  ([^Consumer consumer ^Collection topics]
   (.subscribe consumer topics))
  ([^Consumer consumer ^Collection topics ^ConsumerRebalanceListener rebalance-listener]
   (.subscribe consumer topics rebalance-listener)))

(defn subscribe-to-pattern!
  ([^Consumer consumer ^Pattern topics]
   (.subscribe consumer topics))
  ([^Consumer consumer ^Pattern topics ^ConsumerRebalanceListener rebalance-listener]
   (.subscribe consumer topics rebalance-listener)))

(defn poll!
  "Fetch records. This is where auto-commit and partition reassignment happen."
  ^ConsumerRecords
  [^Consumer consumer ^Duration timeout]
  (.poll consumer timeout))

(defn deprecated-poll!
  ^ConsumerRecords
  [^Consumer consumer ^long timeout-ms]
  (.poll consumer timeout-ms))

(defn commit-async!
  [^Consumer consumer ^Map offsets ^OffsetCommitCallback callback]
  (.commitAsync consumer offsets callback))

(defn wakeup!
  "Call from any thread to release consumer from a long poll before closing."
  [^Consumer consumer]
  (.wakeup consumer))

(defn close!
  "Will block at most timeout-ms milliseconds.
  Will commit offsets of last poll (when auto-commit enabled).
  Should be called after poll returns or wakes up."
  [^Consumer consumer timeout-ms]
  (.close consumer (Duration/ofMillis timeout-ms)))
