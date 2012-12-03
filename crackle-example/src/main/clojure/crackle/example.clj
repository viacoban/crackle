(ns crackle.example
  (:require [crackle.from :as from])
  (:require [crackle.to :as to])
  (:use crackle.core))

;====== word count example ===============
(fn-mapcat split-words [line re] :strings
  (clojure.string/split line re))

(defn count-words [input-path output-path]
  (do-pipeline (from/text-file input-path)
    (split-words #"\s+")
    (p-count)
    (to/text-file output-path)))

;====== average bytes by ip example ======
(fn-map parse-line [line] [:strings :clojure]
  (let [[address bytes] (clojure.string/split line #"\s+")]
    (pair-of address [(read-string bytes) 1])))

(fn-combine sum-bytes-and-counts [value1 value2]
  (mapv + value1 value2))

(fn-mapv compute-average [[bytes requests]] :ints
  (int (/ bytes requests)))

(defn count-bytes-by-ip [input-path output-path]
  (do-pipeline (from/text-file input-path)
    (parse-line)
    (p-group-by-key)
    (sum-bytes-and-counts)
    (compute-average)
    (to/text-file output-path)))
