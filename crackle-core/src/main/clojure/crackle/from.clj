(ns crackle.from
  (:import [org.apache.crunch.io From]))

(defn text-file [path]
  (fn [pipeline] (.readTextFile pipeline path)))
