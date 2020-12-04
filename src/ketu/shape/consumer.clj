(ns ketu.shape.consumer
  (:require [clojure.string]))

(defn- dispatch
  "Dispatch value for the shape multimethods.
  Currently shapes that are a single keyword dispatch on themselves and
  sequential shapes dispatch on their first item.
  Examples:
  :value dispatches on :value.
  [:vector :key :value] dispatches on :vector."
  [_ctx shape]
  (if (sequential? shape)
    (first shape)
    shape))

(defmulti ->data-fn dispatch :default ::default)

(def registry (atom {:field ->data-fn
                     :vector ->data-fn
                     :map ->data-fn}))

(defn register! [shape-name f]
  (swap! registry assoc shape-name f))

(defn ->camelCase [s]
  (clojure.string/replace s #"-[a-z]" (fn [[_ c]] (clojure.string/upper-case c))))

(defn ->member' [k]
  (-> k name ->camelCase symbol))

(defmacro ->access' [r k]
  `(list '. ~r (->member' ~k)))

(defn ->access-list' [r ks]
  (map #(->access' r %) ks))

(defmethod ->data-fn :value [ctx _]
  (->data-fn ctx [:field :value]))

(defmethod ->data-fn :key-value-vector [ctx _]
  (->data-fn ctx [:vector :key :value]))

(defmethod ->data-fn :key-value-map [ctx _]
  (->data-fn ctx [:map :key :value]))

(defmethod ->data-fn :legacy/map [ctx _]
  (->data-fn ctx [:map :key :value :topic :partition :offset]))

(defmethod ->data-fn :field [_ctx [_ k]]
  (eval
    `(fn ~'[^org.apache.kafka.clients.consumer.ConsumerRecord r]
       ~(->access' 'r k))))

(defmethod ->data-fn :vector [_ctx [_ & ks]]
  (eval
    `(fn ~'[^org.apache.kafka.clients.consumer.ConsumerRecord r]
       [~@(->access-list' 'r ks)])))

(defmethod ->data-fn :map [_ctx [_ & ks]]
  (eval
    `(fn ~'[^org.apache.kafka.clients.consumer.ConsumerRecord r]
       ~(zipmap ks (->access-list' 'r ks)))))

(defmethod ->data-fn ::default [ctx [shape-name & _params :as shape]]
  (let [shape-fn (get @registry shape-name)]
    (shape-fn ctx shape)))
