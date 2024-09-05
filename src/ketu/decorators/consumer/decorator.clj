(ns ketu.decorators.consumer.decorator
  (:require [ketu.decorators.consumer.protocol :as cdp]))

(defn- valid? [consumer-decorator consumer-opts]
  (when (not (cdp/valid? consumer-decorator consumer-opts))
    (throw (Exception. "Consumer decorator validation failed"))))

(defn decorate-poll-fn
  [consumer-ctx poll-fn {:keys [ketu.source/consumer-decorator] :as consumer-opts}]
  (valid? consumer-decorator consumer-opts)
  #(cdp/poll! consumer-decorator consumer-ctx poll-fn))
