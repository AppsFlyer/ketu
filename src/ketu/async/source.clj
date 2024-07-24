(ns ketu.async.source
  (:require [clojure.core.async :as async]
            [ketu.async.util :as util]
            [ketu.clients.consumer :as consumer]
            [ketu.shape.consumer :as shape]
            [ketu.spec]
            [ketu.util.log :as log])
  (:import (java.time Duration)
           (org.apache.kafka.clients.consumer Consumer)
           (org.apache.kafka.common.errors WakeupException)
           (org.apache.kafka.common.serialization Deserializer)
           (org.slf4j Logger LoggerFactory)))

(def ^Logger logger (LoggerFactory/getLogger (str *ns*)))

(defn- put-or-abort-pending!
  "Put record on out-chan, block if buffer is full, release block if abort-pending chan is closed.
  Favors putting on buffer if not full, even if abort-pending chan is closed."
  [out-chan item abort-pending]
  (async/alts!! [[out-chan item] abort-pending] :priority true))

(defn finalize-apache-config
  "Returns opts with final :ketu.apache.consumer/config entry"
  [opts]
  (let [original-config (:ketu.apache.consumer/config opts)
        config (util/set-ketu-to-apache-opts original-config opts)]
    (assoc opts :ketu.apache.consumer/config config)))

(defn default-consumer-supplier [opts]
  (let [key-type (:ketu.source/key-type opts)
        key-deserializer (when (instance? Deserializer key-type) key-type)
        value-type (:ketu.source/value-type opts)
        value-deserializer (when (instance? Deserializer value-type) value-type)]
    (consumer/consumer (:ketu.apache.consumer/config opts) key-deserializer value-deserializer)))

(defn default-opts []
  {:ketu.source/consumer-supplier default-consumer-supplier
   :ketu.source/key-type :byte-array
   :ketu.source/value-type :byte-array
   :ketu.source/poll-timeout-ms 100
   :ketu.source/done-putting-timeout-ms 60000
   :ketu.source/consumer-close-timeout-ms 60000
   :ketu.source/consumer-thread-timeout-ms 60000
   :ketu.source/close-out-chan? true
   :ketu.source/close-consumer? true})

(defn- finalize-opts [opts]
  (-> (default-opts)
      (merge opts)
      (finalize-apache-config)))

(defn- subscribe-fn
  "Returns a function that takes a consumer and subscribes to either a topic list or a pattern according to opts."
  [opts]
  (let [subscribe (cond
                    (:ketu.source/topic opts) consumer/subscribe-to-topic!
                    (:ketu.source/topic-list opts) consumer/subscribe-to-list!
                    (:ketu.source/topic-pattern opts) consumer/subscribe-to-pattern!)
        topic (or (:ketu.source/topic opts)
                  (:ketu.source/topic-list opts)
                  (:ketu.source/topic-pattern opts))
        create-listener (:ketu.source/create-rebalance-listener-obj opts)]
    (if create-listener
      (fn [consumer] (subscribe consumer topic (create-listener {:ketu.source/consumer consumer})))
      (fn [consumer] (subscribe consumer topic)))))

(defn- assign-fn
  "Returns a function that takes a consumer and assigns specific partitions according to opts."
  [opts]
  (when-let [assign-tps (:ketu.source/assign-single-topic-partitions opts)]
    (let [topic (:ketu.source.assign/topic assign-tps)
          partitions (:ketu.source.assign/partition-nums assign-tps)]
      (fn [consumer]
        (consumer/assign! consumer (consumer/topic-partitions topic partitions))))))

(defn- poll-fn [^Consumer consumer should-poll? opts]
  (when @should-poll?
    (let [source-name           (:ketu/name opts)
          poll-timeout-duration (Duration/ofMillis (:ketu.source/poll-timeout-ms opts))
          catching-poll?        (:ketu.source.legacy/catching-poll? opts)]
      (if catching-poll?
        ;TODO Eliminate catching poll ASAP.
        ; Just in case of a production issue and generic error handling wasn't implemented yet.
        (fn []
          (try
            (consumer/poll! consumer poll-timeout-duration)
            (catch Exception e
              (log/error logger "[source={}] Caught poll exception" source-name e)
              [])))
        (fn []
          (consumer/poll! consumer poll-timeout-duration))))))

(defn- ->data-fn [{:keys [ketu.source/shape] :as opts}]
  (cond
    (fn? shape) shape
    (some? shape) (shape/->data-fn opts shape)
    :else identity))

(defn- source-existing-consumer
  [^Consumer consumer out-chan opts]
  (let [source-name (:ketu/name opts)
        ^String thread-name (str "ketu-source-" source-name)
        close-out-chan? (:ketu.source/close-out-chan? opts)
        ^long close-consumer? (:ketu.source/close-consumer? opts)
        consumer-close-timeout-ms (:ketu.source/consumer-close-timeout-ms opts)
        should-poll? (volatile! true)
        decorator-fn (some-> (:ketu.source/consumer-decorator opts)
                             (partial {:ketu.source/consumer consumer
                                       :ketu.source/should-poll? should-poll?}))

        abort-pending-put (async/chan)
        done-putting (async/chan)

        subscribe! (or (subscribe-fn opts) (assign-fn opts))
        poll-impl (poll-fn consumer should-poll? opts)
        poll! (if (some? decorator-fn)
                (partial decorator-fn poll-impl)
                poll-impl)
        ->data (->data-fn opts)
        put! (fn [record] (put-or-abort-pending! out-chan (->data record) abort-pending-put))

        consumer-thread
        (async/thread
          (try
            (.info logger "[source={}] Start consumer thread" source-name)
            (.setName (Thread/currentThread) thread-name)

            (subscribe! consumer)

            (loop []
              (when-let [records (poll!)]
                (run! put! records)
                (recur)))

            (catch WakeupException e
              ; We wakeup the consumer on graceful shutdown after should-poll? is false.
              ; If it's not false, somebody else woke the consumer up unexpectedly.
              (when @should-poll?
                (log/error logger "[source={}] Unexpected consumer wakeup" source-name e)))
            (catch Exception e
              (log/error logger "[source={}] Unrecoverable consumer error" source-name e))
            (finally
              (log/info logger "[source={}] Done consuming" source-name)
              (async/close! done-putting)
              (when close-out-chan?
                (log/info logger "[source={}] Close out channel" source-name)
                (async/close! out-chan))
              (when close-consumer?
                (log/info logger "[source={}] Close consumer" source-name)
                (consumer/close! consumer consumer-close-timeout-ms))
              (log/info logger "[source={}] Exit consumer thread" source-name))))


        state {:ketu/source-opts opts
               :ketu.source/out-chan out-chan
               :ketu.source/consumer consumer
               :ketu.source/should-poll? should-poll?
               :ketu.source/abort-pending-put abort-pending-put
               :ketu.source/done-putting done-putting
               :ketu.source/consumer-thread consumer-thread}]

    state))

(defn source
  "Starts consuming into a channel.
  Returns a state map including out-chan. Pass it to the `stop!` function when done."
  [ch opts]
  (let [opts (ketu.spec/assert-and-conform :ketu/public-source-opts opts)
        opts (finalize-opts opts)
        source-name (:ketu/name opts)
        consumer-supplier (:ketu.source/consumer-supplier opts)
        consumer (try
                   (consumer-supplier opts)
                   (catch Exception e
                     (log/error logger "[source={}] Error creating consumer-source" source-name e)
                     (when (opts :ketu.source/close-out-chan?)
                       (log/warn logger "[source={}] Close consumer channel" source-name)
                       (async/close! ch))
                     (throw e)))]
    (try
      (source-existing-consumer consumer ch opts)
      (catch Exception e
        (log/error logger "[sink={}] Error creating source" source-name e)
        (when (:ketu.source/close-consumer? opts)
          (log/warn logger "[source={}] Close consumer" source-name)
          (consumer/close! consumer (:ketu.source/consumer-close-timeout-ms opts)))
        (when (opts :ketu.source/close-out-chan?)
          (log/warn logger "[source={}] Close consumer channel" source-name)
          (async/close! ch))
        (throw e)))))

(defn- no-more-polls! [state]
  (let [should-poll? (:ketu.source/should-poll? state)]
    (vreset! should-poll? false)))

(defn- wakeup-blocked-poll! [state]
  (let [consumer (:ketu.source/consumer state)]
    (consumer/wakeup! consumer)))

(defn- wait-for-put! [state]
  (let [done-putting (:ketu.source/done-putting state)
        done-putting-timeout-ms (-> state :ketu/source-opts :ketu.source/done-putting-timeout-ms)
        timeout (async/timeout done-putting-timeout-ms)]
    (async/alts!! [done-putting timeout] :priority true)))

(defn- abort-pending-put! [state]
  (async/close! (:ketu.source/abort-pending-put state)))

(defn- close-out-chan!
  [state]
  (let [out-chan (:ketu.source/out-chan state)]
    (when (-> state :ketu/source-opts :ketu.source/close-out-chan?)
      (async/close! out-chan))))

(defn done-chan [state]
  (:ketu.source/consumer-thread state))

(defn- wait-for-the-thread! [state]
  (let [consumer-thread (:ketu.source/consumer-thread state)
        consumer-thread-timeout-ms (-> state :ketu/source-opts :ketu.source/consumer-thread-timeout-ms)
        timeout (async/timeout consumer-thread-timeout-ms)]
    (async/alts!! [consumer-thread timeout] :priority true)))

(defn stop! [state]
  (no-more-polls! state)
  (wakeup-blocked-poll! state)
  (wait-for-put! state)
  (abort-pending-put! state)
  (close-out-chan! state)
  (wait-for-the-thread! state))
