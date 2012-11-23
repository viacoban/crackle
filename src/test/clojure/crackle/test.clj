(ns crackle.test
  (:use clojure.test)
  (:use crackle.core)
  (:use crackle.generator)
  (:import [org.apache.crunch Pair]))

(defn split-string [s]
  (clojure.string/split s #"\s+"))

(defn mock-emitter [values]
  (reify org.apache.crunch.Emitter
    (emit [this v] (swap! values conj v))
    (flush [this] nil)))

(defn execute-do-fn [do-fn input]
  (let [values (atom [])]
    (.process (.newInstance do-fn) input (mock-emitter values))
    @values))

(deftest test-do-fn
  (is (= ["hello" "world"] (execute-do-fn (gen-do-fn split-string) "hello world")))
  (is (= ["hello world"] (execute-do-fn (gen-do-fn identity) "hello world")))
  (is (= ["hello world"] (execute-do-fn (gen-do-fn list) "hello world")))
  (is (= ["hello world"] (execute-do-fn (gen-do-fn identity) #{"hello world"})))
  (is (= [{"a" "hello world"}] (execute-do-fn (gen-do-fn identity) {"a" "hello world"}))))

(deftest test-map-fn
  (is (= [["hello" "world"]] (execute-do-fn (gen-map-fn split-string) "hello world")))
  (is (= ["hello world"] (execute-do-fn (gen-map-fn identity) "hello world")))
  (is (= [(pair-of "hello" "world")] (execute-do-fn (gen-map-fn identity) (pair-of "hello" "world"))))
  (is (= [nil] (execute-do-fn (gen-map-fn identity) nil))))

(deftest test-mapv-fn
  (is (= [(pair-of "k" "1")] (execute-do-fn (gen-mapv-fn str) (pair-of "k" 1)))))

(deftest test-combine-fn
  (is (= [(pair-of "k" 6)] (execute-do-fn (gen-combine-fn +) (pair-of "k" [1 2 3])))))

(deftest test-filter-fn
  (is (= [1] (execute-do-fn (gen-filter-fn (partial < 0)) 1)))
  (is (= [] (execute-do-fn (gen-filter-fn (partial < 0)) -1))))



