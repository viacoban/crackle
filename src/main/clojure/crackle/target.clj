(ns crackle.target
  (:import [org.apache.crunch.io To]))

(defn to-text-file [path]
  (To/textFile path))
