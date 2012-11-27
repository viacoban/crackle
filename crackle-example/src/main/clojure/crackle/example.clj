(ns crackle.example
  (:import [crackle.types Clojure])
  (:import [org.apache.crunch.io From To])
  (:import [org.apache.crunch.types.writable Writables])
  (:use crackle.core))

;====== word count example ===============
(defn split-words [f line]
  (doseq [word (clojure.string/split line #"\s+")] (f word)))

(defn count-words [input-path output-path]
  (mr-pipeline (From/textFile input-path)
    (parallelDo (def-dofn `split-words) (Writables/strings))
    (count)
    (write (To/textFile output-path))))

;====== average bytes by ip example ======
(defn parse-line [line]
  (let [parts (clojure.string/split line #"\s+")]
    (pair-of (first parts) [(read-string (second parts)) 1])))

(defn sum-pairs [a b]
  [(+ (first a) (first b)) (+ (second a) (second b))])

(defn compute-average [pair]
  (int (apply / pair)))

(defn count-bytes-by-ip [input-path output-path]
  (mr-pipeline (From/textFile input-path)
    (parallelDo (def-mapfn `parse-line) (Clojure/tableOf))
    (groupByKey)
    (combineValues (def-combinefn `sum-pairs))
    (parallelDo (def-mapvfn `compute-average) (Writables/tableOf (Writables/strings) (Writables/ints)))
    (write (To/textFile output-path))))
