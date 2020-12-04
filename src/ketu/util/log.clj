(ns ketu.util.log
  (:import (org.slf4j Logger)))

(defmacro error
  ([logger s] `(.error ~logger ~s))
  ([logger s x] `(.error ~logger ~s ~x))
  ([logger s x y] `(.error ~logger ~s ~x ~y))
  ([logger s x y & zs]
   `(.error ^Logger ~logger ~s
            ^"[Ljava.lang.Object;" (into-array Object ~(into [x y] zs)))))

(defmacro warn
  ([logger s] `(.warn ~logger ~s))
  ([logger s x] `(.warn ~logger ~s ~x))
  ([logger s x y] `(.warn ~logger ~s ~x ~y))
  ([logger s x y & zs]
   `(.warn ^Logger ~logger ~s
           ^"[Ljava.lang.Object;" (into-array Object ~(into [x y] zs)))))

(defmacro info
  ([logger s] `(.info ~logger ~s))
  ([logger s x] `(.info ~logger ~s ~x))
  ([logger s x y] `(.info ~logger ~s ~x ~y))
  ([logger s x y & zs]
   `(.info ^Logger ~logger ~s
           ^"[Ljava.lang.Object;" (into-array Object ~(into [x y] zs)))))
