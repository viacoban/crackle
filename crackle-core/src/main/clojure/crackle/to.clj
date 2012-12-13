(ns crackle.to
;  (:use crackle.impl.types)
  (:import [org.apache.crunch.types.writable WritableTypeFamily])
  (:import [org.apache.crunch.fn IdentityFn])
  (:import [org.apache.crunch.io To At]))

(defn as-writable [type]
  (.as (WritableTypeFamily/getInstance) type))

(defn text-file [path]
  (fn [pcoll]
    (-> pcoll
      (.parallelDo "asText" (IdentityFn/getInstance) (as-writable (.getPType pcoll)))
      (.write (At/textFile path)))))

(defn seq-file [path]
  (fn [pcoll]
    (if (isa? (class pcoll) org.apache.crunch.PTable)
      (.write pcoll (At/sequenceFile path (as-writable (.getKeyType pcoll)) (as-writable (.getValueType pcoll))))
      (.write pcoll (At/sequenceFile path (as-writable (.getPType pcoll)))))))
