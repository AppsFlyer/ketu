(ns ketu.decorators.consumer.protocol)

(defprotocol ConsumerDecorator
  (poll! [this consumer-ctx poll-fn] "fn [consumer-context poll-fn] -> Iterable<ConsumerRecord>")
  (validate [this consumer-opts] "Returns true if consumer-opts are valid according to the decorator logic"))
