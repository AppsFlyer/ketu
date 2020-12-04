(ns ketu.test.log
  (:require [clojure.string])
  (:import (java.util.concurrent ConcurrentLinkedQueue)
           (ch.qos.logback.classic Level Logger)
           (ch.qos.logback.classic.spi ILoggingEvent)
           (ch.qos.logback.core AppenderBase)
           (org.slf4j LoggerFactory)))

(defn- ->level [level-keyword]
  (-> level-keyword
      name
      clojure.string/upper-case
      Level/toLevel))

(defn root-logger []
  (LoggerFactory/getLogger Logger/ROOT_LOGGER_NAME))

(defn ns-logger [ns]
  (LoggerFactory/getLogger (name ns)))

(defn set-level [^Logger logger level-keyword]
  (.setLevel logger (->level level-keyword)))

(defn with-test-appender
  ([^Logger logger f]
   (with-test-appender logger :debug f))
  ([^Logger logger level-keyword f]
   (let [original-level (.getLevel logger)
         events (ConcurrentLinkedQueue.)
         appender (proxy [AppenderBase] []
                    (append [event] (.add events event)))]
     (try
       (.start appender)
       (.addAppender logger appender)
       (set-level logger level-keyword)
       (f {::events events})
       (finally
         (.stop appender)
         (.detachAppender logger appender)
         (.setLevel logger original-level))))))

(defn events [{::keys [events]}]
  (let [->keyword (fn [^Level level] (-> level clojure.string/lower-case keyword))]
    (mapv (fn [^ILoggingEvent e] [(->keyword (.getLevel e)) (.getFormattedMessage e)]) events)))
