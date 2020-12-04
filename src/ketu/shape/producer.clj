(ns ketu.shape.producer
  (:import (org.apache.kafka.clients.producer ProducerRecord)))

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

(defmulti ->record-fn dispatch :default ::default)

(def registry (atom {:vector ->record-fn
                     :map ->record-fn}))

(defn register! [shape-name f]
  (swap! registry assoc shape-name f))

(defn requires-topic?
  "Does the producer input shape require a top-level producer topic
  since it doesn't provide topic as a field"
  [shape]
  (boolean
    (or (#{:value :key-value-vector :key-value-map} shape)
        (and (sequential? shape)
             (not (some #{:topic} shape))))))

(defn explain-shape
  ^String
  [topic shape]
  (when (and (requires-topic? shape) (nil? topic))
    (str "Missing topic option (required for shape " shape ")")))

(defn assert-shape [{topic :ketu/topic} shape]
  (if-some [explanation (explain-shape topic shape)]
    (throw (Exception. explanation))
    shape))

(defmethod ->record-fn :value [{topic :ketu/topic :as ctx} shape]
  (assert-shape ctx shape)
  (fn [v] (ProducerRecord. topic v)))

(defmethod ->record-fn :key-value-vector [ctx shape]
  (assert-shape ctx shape)
  (->record-fn ctx [:vector :key :value]))

(defmethod ->record-fn :key-value-map [ctx shape]
  (assert-shape ctx shape)
  (->record-fn ctx [:map :key :value]))

(defmethod ->record-fn :vector [ctx [_ & ks :as shape]]
  (assert-shape ctx shape)
  (let [topic (:ketu/topic ctx)
        k->i (into {} (map-indexed (fn [i k] [k i])) ks)
        ctor-ks [:topic :partition :timestamp :key :value :headers]
        [topic-i partition-i timestamp-i & rest-is] (map #(k->i %) ctor-ks)]
    (eval
      `(fn ~'[x]
         (ProducerRecord.
           ~(or (some->> topic-i (list 'nth 'x)) topic)
           ~(when partition-i `(int (nth ~'x ~partition-i)))
           ~(when timestamp-i `(long (nth ~'x ~timestamp-i)))
           ~@(map #(when % (list 'nth 'x %)) rest-is))))))

(defmethod ->record-fn :map [ctx [_ & ks :as shape]]
  (assert-shape ctx shape)
  (let [topic (:ketu/topic ctx)
        kset (set ks)]
    (eval
      `(fn ~'[x]
         (ProducerRecord.
           ~(if (kset :topic) `(~'x :topic) topic)
           ~(when (kset :partition) `(int (~'x :partition)))
           ~(when (kset :timestamp) `(long (~'x :timestamp)))
           ~@(map #(list 'x %) [:key :value :headers]))))))

(defmethod ->record-fn ::default [ctx [shape-name & _params :as shape]]
  (let [shape-fn (get @registry shape-name)]
    (when (nil? shape-fn)
      (throw (ex-info "Unregistered shape" {::shape shape})))
    (shape-fn ctx shape)))
