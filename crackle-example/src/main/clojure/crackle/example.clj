(ns crackle.example
  (:use crackle.core)
  (:use crackle.source)
  (:use crackle.target)
  (:require [crackle.types :as t]))

;====== word count example ===============
(defn split-words [f line]
  (doseq [word (clojure.string/split line #"\s+")] (f word)))

(defn count-words [input-path output-path]
  (mr-pipeline (from-text-file input-path)
    (:parallelDo (def-dofn `split-words) (t/strings))
    (:count)
    (:write (to-text-file output-path))))

;====== average bytes by ip example ======
(defn parse-line [line]
  (let [parts (clojure.string/split line #"\s+")]
    (pair-of (first parts) [(read-string (second parts)) 1])))

(defn sum-pairs [a b]
  [(+ (first a) (first b)) (+ (second a) (second b))])

(defn compute-average [pair]
  (int (apply / pair)))

(defn count-bytes-by-ip [input-path output-path]
  (mr-pipeline (from-text-file input-path)
    (:parallelDo (def-mapfn `parse-line) (t/table-of-binary))
    (:groupByKey)
    (:combineValues (def-combinefn `sum-pairs))
    (:parallelDo (def-mapvfn `compute-average) (t/w-table-of (t/strings) (t/wints)))
    (:write (to-text-file output-path))))
