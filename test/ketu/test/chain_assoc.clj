(ns ketu.test.chain-assoc
  (:require [sieppari.core :as s]))

(defn- associng-enter
  "Returns an interceptor that assocs the result of (f ctx) to ctx on key k."
  [[k f]]
  {:enter (fn [ctx]
            (assoc ctx k (f ctx)))})

(defn chain-assoc
  "Similar to assoc but with an error-handler and functions instead of values.
  Start with an initial context map `m`.
  For each pair, call the function with the accumulated map from previous pairs, and assoc the result with the key.
  Throwing exception at any point will call on-error with the accumulated context and error as params,
  and will then rethrow.
  Use on-error to close resources in case of error."
  [m on-error & chain]
  (s/execute-context
    (into [{:error (fn [ctx] (doto ctx (on-error (:error ctx))))}]
          (comp (partition-all 2) (map associng-enter))
          chain)
    m))
