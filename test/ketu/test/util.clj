(ns ketu.test.util
  (:require [clojure.core.async :as async]
            [clojure.string])
  (:import (java.util.concurrent TimeoutException)))

(defn try-put! [ch v]
  (let [timeout-ch (async/timeout 5000)
        [put-result completed-ch] (async/alts!! [[ch v] timeout-ch])]
    (when (= completed-ch timeout-ch)
      (throw (TimeoutException. "Taking from channel timed out")))
    put-result))

(defn try-take! [ch]
  (let [timeout-ch (async/timeout 5000)
        [item completed-ch] (async/alts!! [ch timeout-ch])]
    (when (= completed-ch timeout-ch)
      (throw (TimeoutException. "Taking from channel timed out")))
    item))
