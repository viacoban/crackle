(ns crackle.example
  (:require [crackle.from :as from])
  (:require [crackle.to :as to])
  (:use crackle.core))

;====== word count example ===============
(fn-mapcat split-words [line] :strings
  (clojure.string/split line #"\s+"))

(defn count-words [input-path output-path]
  (pipeline (from/text-file input-path)
    (split-words)
    (count-values)
    (to/text-file output-path)))

;====== average bytes by ip example ======
(fn-map parse-line [line] [:strings :clojure]
  (let [parts (clojure.string/split line #"\s+")]
    (pair-of (first parts) [(read-string (second parts)) 1])))

(fn-combine sum-bytes-and-counts [value1 value2]
  [(+ (first value1) (first value2)) (+ (second value1) (second value2))])

(fn-mapv compute-average [value] :ints
  (int (apply / value)))

(defn count-bytes-by-ip [input-path output-path]
  (pipeline (from/text-file input-path)
    (parse-line)
    (group-by-key)
    (sum-bytes-and-counts)
    (compute-average)
    (to/text-file output-path)))
