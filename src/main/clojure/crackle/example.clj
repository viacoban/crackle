(ns crackle.example
  (:use crackle.core))

;====== word count example ===============
(defn split-words [line]
  (clojure.string/split line #"\s+"))

(defn count-words [input-path output-path]
  (mem-pipeline (from-txt input-path)
    (:parallelDo (do-fn split-words) simple-ptype)
    (:count)
    (:write (to-txt output-path))))

;====== average bytes by ip example ======
(defn parse-line [line]
  (let [parts (split-words line)]
    (pair-of (first parts) [(read-string (second parts)) 1])))

(defn sum-pairs [a b]
  [(+ (first a) (first b)) (+ (second a) (second b))])

(defn compute-average [sum-and-count-pair]
  (apply / sum-and-count-pair))

(defn count-bytes-by-ip [input-path output-path]
  (mem-pipeline (from-txt input-path)
    (:parallelDo (do-fn parse-line) table-ptype)
    (:groupByKey)
    (:combineValues (combine-fn sum-pairs))
    (:parallelDo (mapv-fn compute-average) table-ptype)
    (:write (to-txt output-path))))
