(ns ketu.async.sink
  (:require [clojure.core.async :as async]
            [ketu.async.util :as util]
            [ketu.clients.producer :as producer]
            [ketu.shape.producer :as shape]
            [ketu.spec]
            [ketu.util.log :as log])
  (:import (org.apache.kafka.common.errors TimeoutException)
           (org.apache.kafka.common.serialization Serializer)
           (org.slf4j Logger LoggerFactory)))

(def ^Logger logger (LoggerFactory/getLogger (str *ns*)))

(defn- item-callback-creator
  [state]
  (let [callback (-> state :ketu/sink-opts :ketu.sink/callback)
        callback-obj (-> state :ketu/sink-opts :ketu.sink/callback-obj)
        create-callback (-> state :ketu/sink-opts :ketu.sink/create-callback)
        create-callback-obj (-> state :ketu/sink-opts :ketu.sink/create-callback-obj)]
    (cond
      callback (let [cb (-> callback producer/callback)] (fn [_] cb))
      callback-obj (fn [_] callback-obj)
      create-callback (fn [item] (-> item create-callback producer/callback))
      create-callback-obj (fn [item] (-> item create-callback-obj))
      :else (fn [_] nil))))

(defn- abort-or-take!
  "Takes from input-chan, blocks if nothing in it.
  Returns nil immediately when abort-input is closed even if inputs are available."
  [input-chan abort-input]
  (let [[v _] (async/alts!! [abort-input input-chan] :priority true)]
    v))

(defn- send-fn [state]
  (let [sink-name (-> state :ketu/sink-opts :ketu/name)
        producer (:ketu.sink/producer state)
        catching-send? (-> state :ketu/sink-opts :ketu.sink.legacy/catching-send?)]
    (if catching-send?
      ;TODO Eliminate catching poll ASAP.
      ; Just in case of a production issue and generic error handling wasn't implemented yet.
      (fn [record callback]
        (try
          (producer/send! producer record callback)
          (catch TimeoutException e
            (log/error logger "[sink={}] Send timeout'" sink-name e))
          (catch Exception e
            (log/error logger "[sink={}] Caught send exception" sink-name e))))
      ; This one catches timeouts only. Other exceptions will terminate the worker.
      (fn [record callback]
        (try
          (producer/send! producer record callback)
          (catch TimeoutException e
            (log/error logger "[sink={}] Send timeout" sink-name e)))))))

(defn- send-loop
  [state]
  (let [in-chan (:ketu.sink/in-chan state)
        abort-input (:ketu.sink/abort-input state)
        ->record (:ketu.sink/->record state)
        ->callback (item-callback-creator state)
        send! (send-fn state)]
    (loop []
      (when-some [item (abort-or-take! in-chan abort-input)]
        (let [record (->record item)
              callback (->callback item)]
          (send! record callback))
        (recur)))))

(defn done-chan [state]
  (:ketu.sink/sender-threads-done state))

(defn- wait-until-done-or-timeout!
  [state]
  (let [done (done-chan state)
        timeout-ms (-> state :ketu/sink-opts :ketu.sink/sender-threads-timeout-ms)
        timeout (async/timeout timeout-ms)]
    (async/alts!! [done timeout])))

(defn- abort-input!
  [state]
  (let [abort-input (:ketu.sink/abort-input state)]
    (async/close! abort-input)))

(defn- close-producer!
  [state]
  (when (-> state :ketu/sink-opts :ketu.sink/close-producer?)
    (let [sink-name (-> state :ketu/sink-opts :ketu/name)
          producer (:ketu.sink/producer state)
          producer-close-timeout-ms (-> state :ketu/sink-opts :ketu.sink/producer-close-timeout-ms)]
      (log/info logger "[sink={}] Close producer" sink-name)
      (producer/close! producer producer-close-timeout-ms))))

(defn- ->record-fn [{:keys [ketu.sink/shape] :as opts}]
  (cond
    (fn? shape) shape
    (some? shape) (shape/->record-fn opts shape)
    :else identity))

(defn finalize-apache-config
  "Returns opts with final :ketu.apache.producer/config entry"
  [opts]
  (let [original-config (:ketu.apache.producer/config opts)
        config (util/set-ketu-to-apache-opts original-config opts)]
    (assoc opts :ketu.apache.producer/config config)))

(defn default-producer-supplier [opts]
  (let [key-type (:ketu.sink/key-type opts)
        key-serializer (when (instance? Serializer key-type) key-type)
        value-type (:ketu.sink/value-type opts)
        value-serializer (when (instance? Serializer value-type) value-type)]
    (producer/producer (:ketu.apache.producer/config opts) key-serializer value-serializer)))

(defn default-opts []
  {:ketu.sink/producer-supplier default-producer-supplier
   :ketu.sink/key-type :byte-array
   :ketu.sink/value-type :byte-array
   :ketu.sink/sender-threads-num 1
   :ketu.sink/producer-close-timeout-ms 60000
   :ketu.sink/sender-threads-timeout-ms 60000
   :ketu.sink/close-producer? true})

(defn- finalize-opts [opts]
  (-> (default-opts)
      (merge opts)
      (finalize-apache-config)))

(defn- sink-existing-producer
  [producer in-chan opts]
  (let [sink-name (:ketu/name opts)
        sender-threads-num (:ketu.sink/sender-threads-num opts)
        remaining-sender-threads (atom sender-threads-num)
        close-producer? (:ketu.sink/close-producer? opts)
        ->record (->record-fn opts)

        abort-input (async/chan)

        state {:ketu/sink-opts opts
               :ketu.sink/in-chan in-chan
               :ketu.sink/->record ->record
               :ketu.sink/producer producer
               :ketu.sink/abort-input abort-input}

        threads-done-chan (async/chan)

        ;; Channels that are closed when their thread loop is done.
        _ (log/info logger "[sink={}] Start {} worker(s)" sink-name sender-threads-num)
        sender-threads
        (mapv #(let [thread-name (str "ketu-sink-" sink-name "-" %)]
                  (async/thread
                    (try
                      (log/info logger "[sink={} worker={}] Start" sink-name %)
                      (.setName (Thread/currentThread) thread-name)
                      (send-loop state)
                      (catch Exception e
                        (log/error logger "[sink={} worker={}] Unrecoverable error" sink-name % e))
                      (finally
                        (log/info logger "[sink={} worker={}] Exit" sink-name %)))
                    (when (zero? (swap! remaining-sender-threads dec))
                      (when close-producer?
                        (log/info logger "[sink={} auto-close] All workers are done" sink-name)
                        (try
                          (close-producer! state)
                          (catch Throwable e
                            (log/error logger "[sink={} auto-close] Error" sink-name e))))
                      (async/close! threads-done-chan))))
               (range sender-threads-num))]
    (-> state
        (assoc :ketu.sink/sender-threads sender-threads
               :ketu.sink/sender-threads-done threads-done-chan))))

(defn sink
  "Sends ProducerRecord's from in-chan to kafka.
  Closes the producer by default when in-chan is exhausted or when calling `stop!`
  If `:ketu.sink/close-producer?` is set to `false` the user will have to close
  the producer manually."
  [in-chan opts]
  (let [opts (ketu.spec/assert-and-conform :ketu/public-sink-opts opts)
        opts (finalize-opts opts)
        sink-name (:ketu/name opts)
        producer-supplier (:ketu.sink/producer-supplier opts)
        producer (producer-supplier opts)]
    (try
      (sink-existing-producer producer in-chan opts)
      (catch Exception e
        (log/error logger "[sink={}] Error creating sink" sink-name e)
        (when (:ketu.sink/close-producer? opts)
          (log/warn logger "[sink={}] Close producer" sink-name)
          (producer/close! producer (:ketu.sink/producer-close-timeout-ms opts)))
        (throw e)))))

(defn stop!
  "Sends everything in the input channel to kafka and closes the producer.
  Input channel should be closed before calling this function.
  It shouldn't be necessary to call this function since by default the sink closes
  itself automatically when the input channel is closed."
  [state]
  (wait-until-done-or-timeout! state)
  (abort-input! state)
  (close-producer! state))
