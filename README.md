# Ketu

[![Build Status](https://img.shields.io/github/workflow/status/appsflyer/ketu/Build?event=push&branch=master&label=build)](https://github.com/appsflyer/ketu/actions)
[![Coverage Status](https://coveralls.io/repos/github/AppsFlyer/ketu/badge.svg?branch=master)](https://coveralls.io/github/AppsFlyer/ketu?branch=master)
[![Clojars Project](https://img.shields.io/clojars/v/com.appsflyer/ketu.svg)](https://clojars.org/com.appsflyer/ketu)
[![cljdoc badge](https://cljdoc.org/badge/com.appsflyer/ketu)](https://cljdoc.org/d/com.appsflyer/ketu/CURRENT)

A Clojure Apache Kafka client with core.async api

```clojure
[com.appsflyer/ketu "1.1.0"]
```

## Features

* **Channels API**: Take kafka data from a channel and send data to kafka through a channel.
* **Consumer Source**: Polls records from kafka and puts them on a channel.
* **Producer Sink**: Takes records from a channel and sends them to kafka.
* **Shapes**: Transform the original objects of the java client to clojure data and back.
* **Simple Configuration**: Friendly, validated configuration.

## Minimal Example

Consume a name string from kafka and produce a greeting string for that name back into kafka, all through channels:

```clojure
(ns example
  (:require [clojure.core.async :refer [chan close! <!! >!!]]
            [ketu.async.source :as source]
            [ketu.async.sink :as sink]))
  
(let [<names (chan 10)
      source-opts {:name "greeter-consumer"
                   :brokers "broker1:9092"
                   :topic "names"
                   :group-id "greeter"
                   :value-type :string
                   :shape :value}
      source (source/source <names source-opts)

      >greets (chan 10)
      sink-opts {:name "greeter-producer"
                 :brokers "broker2:9091"
                 :topic "greetings"
                 :value-type :string
                 :shape :value}
      sink (sink/sink >greets sink-opts)]

  ;; Consume a name and produce a greeting. You could also do this with e.g. clojure.core.async/pipeline.
  (->> (<!! <names)
       (str "Hi, ")
       (>!! >greets))

  ;; Close the source. It automatically closes the source channel `<names`.
  (source/stop! source)
  ;; Close the sink channel `>greets`. It causes the sink to close itself as a consequence.
  (close! >greets))
```

## Configuration reference

Anything that is not documented is not supported and might change.

Read more about the default values used by the underlying Kafka clients v3.3.1 [here](https://kafka.apache.org/33/documentation.html)

Note: `int` is used for brevity but can also mean `long`. Don't worry about it.

#### Common options (both source and sink accept these)
| Key              | Type                    | Req?     | Notes                                                                            |
|------------------|-------------------------|----------|----------------------------------------------------------------------------------|
| :brokers         | string                  | required | Comma separated `host:port` values e.g "broker1:9092,broker2:9092"               |
| :topic           | string                  | required |                                                                                  |
| :name            | string                  | required | Simple human-readable identifier, used in logs and thread names                  |
| :key-type        | `:string`,`:byte-array` | optional | Default `:byte-array`, used in configuring key serializer/deserializer           |
| :value-type      | `:string`,`:byte-array` | optional | Default `:byte-array`, used in configuring value serializer/deserializer         |
| :internal-config | map                     | optional | A map of the underlying java client properties, for any extra lower level config |

#### Consumer-source options
| Key         | Type                                                                                          | Req?       | Notes                                                                                 |
|-------------|-----------------------------------------------------------------------------------------------|------------|---------------------------------------------------------------------------------------|
| :group-id   | string                                                                                        | required   |                                                                                       |
| :shape      | `:value:`, `[:vector <fields>]`,`[:map <fields>]`, or an arity-1 function of `ConsumerRecord` | optional   | If unspecified, channel will contain ConsumerRecord objects. [Examples](#data-shapes) |
| :ketu.source/consumer-commands-chan | channel | optional | Used for passing custom functions to be executed from within the poll loop. Items of this channel are expected to be of type `fn[x]`. One example for using this channel is to enable pausing/resuming of the underlying kafka consumer, since trying to do that outside the poll loop causes a `ConcurrentModificationException` to be thrown. [Example](#example-of-using-the-custom-commands-channel) |


#### Producer-sink options
| Key               | Type                                                                                                             | Req?       | Notes                                                                                          |
|-------------------|------------------------------------------------------------------------------------------------------------------|------------|------------------------------------------------------------------------------------------------|
| :shape            | `:value`, `[:vector <fields>]`,`[:map <fields>]`, or an arity-1 function of the input returning `ProducerRecord` | optional   | If unspecified, you must put ProducerRecord objects on the channel. [Examples](#data-shapes)   |
| :compression-type | `"none"` `"gzip"` `"snappy"` `"lz4"` `"zstd"`                                                                    | optional   | Default `"none"`, values are same as "compression.type" of the java producer                   |
| :workers          | int                                                                                                              | optional   | Default `1`, number of threads that take from the channel and invoke the internal producer     |

## Data shapes

You don't have to deal with ConsumerRecord or ProducerRecord objects.<br>
To get a clojure data structure with any of the ConsumerRecord fields, configure the consumer shape:
```clojure
; Value only:
{:topic "names"
 :key-type :string
 :value-type :string
 :shape :value}
(<!! consumer-chan)
;=> "v"

; Vector:
{:shape [:vector :key :value :topic]}
(<!! consumer-chan)
;=> ["k" "v" "names"]

; Map
{:shape [:map :key :value :topic]}
(<!! consumer-chan)
;=> {:key "k", :value "v", :topic "names"}
```
Similarly, to put a clojure data structure on the producer channel:
```clojure
; Value only:
{:key-type :string
 :value-type :string
 :shape :value}
(>!! producer-chan "v")

; Vector:
{:shape [:vector :key :value]}
(>!! producer-chan ["k" "v"])

; Vector with topic in each message:
{:shape [:vector :key :value :topic]}
(>!! producer-chan ["k1" "v1" "names"])
(>!! producer-chan ["k2" "v2" "events"])
```

## Example of using the custom commands channel

In this example we demonstare how to enable pause/resume of the consumer:

```clojure
(ns custom-commands-channel-example
 (:require  [clojure.core.async :as async]
            [ketu.async.source :as source]
            [ketu.async.sink :as sink]))

(let [commands-chan (async/chan 10)
      consumer-chan (async/chan 10)
      consumer-opts {:name                               "consumer-example"
                     :brokers                            "broker1:9092"
                     :topic                              "example"
                     :group-id                           "example"
                     :value-type                         :string
                     :shape                              :value
                     :ketu.source/consumer-commands-chan commands-chan}
      source (source/source consumer-chan consumer-opts)

      producer-chan (async/chan 10)
      sink-opts {:name       "producer-example"
                 :brokers    "broker1:9092"
                 :topic      "example"
                 :value-type :string
                 :shape      :value}
      sink (sink/sink producer-chan sink-opts)

      ; periodically produce data to the topic
      producing (future
                 (dotimes [i 20]
                  (async/>!! producer-chan (str i))
                  (Thread/sleep 300))
                 (async/>!! producer-chan "done")
                 (async/close! producer-chan))

      ; read from the consumer channel and print to the screen
      processing (future
                  (loop []
                   (let [message (async/<!! consumer-chan)]
                    (println message)
                    (when (not= message "done")
                     (recur)))))]
 (try
  (Thread/sleep 2000) ; consumer is consuming normally
  (let [paused (promise)
        resumed (promise)]
   
   ; Send the commands channel a function that will pause the consumer
   (async/>!! commands-chan (fn [{consumer :ketu.source/consumer}]
                             (.pause consumer (.assignment consumer))
                             (deliver paused true)))
   
   @paused
   (println "consumer is paused")
   (Thread/sleep 2000)

   ; Send the commands channel a function that will resume the consumer
   (async/>!! commands-chan (fn [{consumer :ketu.source/consumer}]
                             (.resume consumer (.paused consumer))
                             (deliver resumed true)))
   
   @resumed
   (println "consumer is resumed")

   ; Wait for all futures to finish
   @producing
   @processing)
  (finally
   (source/stop! source))))
```

## Development & Contribution

We welcome feedback and would love to hear about use-cases other than ours. You can open issues, send pull requests,
or contact us at clojurians slack.
