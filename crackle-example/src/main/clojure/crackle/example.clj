(ns crackle.example
  (:use crackle.core)
  (:require [crackle.from :as from])
  (:require [crackle.to :as to]))

;====== word count example ===============
(defn-mapcat split-words [regexp] :strings
  (fn [line] (clojure.string/split line regexp)))

(defn count-words [input-path output-path]
  (do-pipeline
    (from/text-file input-path)
    (parallel-do! (split-words #"\s+"))
    (count!)
    (to/text-file output-path)))

;;====== average bytes by ip example ======
(defn-map parse-line [regexp] [:strings :clojure]
  (fn [line]
    (let [[address bytes] (clojure.string/split line regexp)]
      (pair-of address [(read-string bytes) 1]))))

(defn-combine sum-bytes-and-counts []
  (fn [value1 value2]
    (mapv + value1 value2)))

(defn-mapv compute-average [] [:strings :ints]
  (fn [[bytes requests]]
    (int (/ bytes requests))))

(defn count-bytes-by-ip [input-path output-path]
  (do-pipeline
    (from/text-file input-path)
    (parallel-do! (parse-line #"\s+"))
    (group-by-key!)
    (combine-values! (sum-bytes-and-counts))
    (parallel-do! (compute-average))
    (to/text-file output-path)))
