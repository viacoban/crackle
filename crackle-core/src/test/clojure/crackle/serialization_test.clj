(ns crackle.serialization-test
  (:use clojure.test))

(def input-fn
  (.getInputMapFn (crackle.BinaryTypes/anything)))

(def output-fn
  (.getOutputMapFn (crackle.BinaryTypes/anything)))

(defn serialize-deserialize [v]
  (.map input-fn (.map output-fn v)))

(deftest test-serialization
  (is (= [1] (serialize-deserialize [1])))
  (is (= {"a" 1} (serialize-deserialize {"a" 1})))
  (is (= :zzz (serialize-deserialize :zzz))))

