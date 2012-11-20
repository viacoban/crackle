(ns crackle.example
  (:use crackle.core))

;====== word count example ===============
(defn split-words [line]
  (clojure.string/split line #"\s+"))

(defn count-words [input-path output-path]
  (mem-pipeline (from-txt input-path) (to-txt output-path)
    (=each-to-seq split-words)
    (=count)))

;====== average bytes by ip ==============
(defn parse-line [line]
  (let [parts (split-words line)]
    (pair-of (first parts) [(second parts) 1])))

(defn sum-pairs [a b]
  [(+ (first a) (first b)) (+ (second a) (second b))])

(defn compute-average [sum-and-count-pair]
  (apply / sum-and-count-pair))

(defn count-bytes-by-ip [input-path output-path]
  (mem-pipeline (from-txt input-path) (to-txt output-path)
    (=each parse-line)
    (=group-by-key)
    (=combine-values sum-pairs)
    (=map-value compute-average)))

