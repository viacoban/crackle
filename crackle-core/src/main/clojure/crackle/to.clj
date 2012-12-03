(ns crackle.to
  (:use crackle.impl.core)
  (:import [org.apache.crunch.types.writable WritableTypeFamily])
  (:import [org.apache.crunch.fn IdentityFn])
  (:import [org.apache.crunch.io To At]))

(defn text-file [path]
  (fn [pcoll]
    (-> pcoll
      (.parallelDo "asText" (IdentityFn/getInstance) (.as (WritableTypeFamily/getInstance) (.getPType pcoll)))
      (.write (At/textFile path)))))
