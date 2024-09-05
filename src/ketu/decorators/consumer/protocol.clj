(ns ketu.decorators.consumer.protocol)

(defprotocol ConsumerDecorator
  "Consumer decorator provides a way to extend the consumer source functionality.
   The decorator runs in the context of the polling thread and allows custom control on the internal consumer instance"
  (poll! [this consumer-ctx poll-fn]
    "Decorates the internal consumer poll loop.
     - Parameters:
       - `consumer-ctx`: A map containing the consumer context, typically {:ketu.source/consumer consumer}.
       - `poll-fn`: A function with no arguments that returns an Iterable of ConsumerRecord.
     - Returns: An iterable collection of ConsumerRecord.
     - The decorator should call the `poll-fn` on behalf of the consumer source.")
  (valid? [this consumer-opts]
    "Validates the consumer options according to the decorator logic.
     - Parameters:
       - `consumer-opts`: A map of consumer options to be validated.
     - Returns: true if the consumer options are valid according to the decorator logic, false otherwise."))
