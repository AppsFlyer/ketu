(ns ketu.test.thrown-root-cause
  (:require [clojure.stacktrace]
            [clojure.test]))

(defmethod clojure.test/assert-expr 'thrown-root-cause-with-msg? [msg form]
  ;; Same as clojure.test/thrown-with-msg? but checks the root cause of the exception.
  (let [klass (nth form 1)
        re (nth form 2)
        body (nthnext form 3)]
    `(try ~@body
          (clojure.test/do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
          (catch Exception e#
            (let [^Throwable r# (clojure.stacktrace/root-cause e#)
                  m# (.getMessage r#)]
              (if (and (isa? (class r#) ~klass) (re-find ~re m#))
                (clojure.test/do-report {:type :pass, :message ~msg,
                                         :expected '~form, :actual e#})
                (clojure.test/do-report {:type :fail, :message ~msg,
                                         :expected '~form, :actual e#})))
            e#))))
