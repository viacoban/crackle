(ns crackle.types-test
  (:use clojure.test)
  (:use crackle.impl.types)
  (:import [crackle.types Clojure])
  (:import [org.apache.crunch.types.writable Writables])
  (:import [org.apache.crunch.types.avro Avros])
  (:import [org.apache.hadoop.io BytesWritable Text])
  )

(defn compare-with-eval [types result]
  (let [resolved (global-type-resolver types)]
    (println "type" types "resolved to" resolved)
    (= (eval resolved) result)))

(deftest writables-test
  (is (compare-with-eval :ints (Writables/ints)))
  (is (compare-with-eval :strings (Writables/strings)))
  (is (compare-with-eval :longs (Writables/longs)))
  (is (compare-with-eval {:type :longs :family :writables} (Writables/longs)))
  (is (compare-with-eval [:strings :ints] (Writables/tableOf (Writables/strings) (Writables/ints))))
  (is (compare-with-eval [:strings :clojure] (Writables/tableOf (Writables/strings) (Clojure/anything))))
  (is (compare-with-eval {:type [:strings :ints] :family :writables} (Writables/tableOf (Writables/strings) (Writables/ints)))))

(deftest avros-test
  (is (compare-with-eval {:type :ints :family :avros} (Avros/ints)))
  (is (compare-with-eval {:type :strings :family :avros} (Avros/strings)))
  (is (compare-with-eval {:type [:strings :ints] :family :avros} (Avros/tableOf (Avros/strings) (Avros/ints)))))


(deftest class-test
  (is (compare-with-eval Text (Writables/writables Text)))
  (is (compare-with-eval BytesWritable (Writables/writables BytesWritable)))
  (is (compare-with-eval {:type Text :family :writables} (Writables/writables Text)))
  (is (compare-with-eval [Text BytesWritable] (Writables/tableOf (Writables/writables Text) (Writables/writables BytesWritable)))))
