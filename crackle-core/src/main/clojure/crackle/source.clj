(ns crackle.source
  (:import [org.apache.crunch.io From]))

(defn from-text-file
  ([path]
    (From/textFile path))
  ([path type]
    (From/textFile path type)))
