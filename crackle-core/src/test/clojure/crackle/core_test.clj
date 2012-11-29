(ns crackle.core-test
  (:use crackle.core)
  (:use clojure.test)
  (:import [org.apache.crunch Pair]))

(defn split-string [f s]
  (doseq [word (clojure.string/split s #"\s+")] (f word)))

(defn mock-emitter [values]
  (reify org.apache.crunch.Emitter
    (emit [this v] (swap! values conj v))
    (flush [this] nil)))

(defn execute-do-fn [do-fn input]
  (let [values (atom [])]
    (.initialize do-fn)
    (.process do-fn input (mock-emitter values))
    @values))

(deftest test-do-fn
  (is (= ["hello" "world"] (execute-do-fn (def-dofn `split-string) "hello world"))))

(deftest test-map-fn
  (is (= ["hello world"] (execute-do-fn (def-mapfn `identity) "hello world")))
  (is (= [(pair-of "hello" "world")] (execute-do-fn (def-mapfn `identity) (pair-of "hello" "world"))))
  (is (= [nil] (execute-do-fn (def-mapfn `identity) nil)))
  (is (= ['("hello world")] (execute-do-fn (def-mapfn `list) "hello world")))
  (is (= [#{"hello world"}] (execute-do-fn (def-mapfn `identity) #{"hello world"})))
  (is (= [{"a" "hello world"}] (execute-do-fn (def-mapfn `identity) {"a" "hello world"}))))

(deftest test-mapv-fn
  (is (= [(pair-of "k" "1")] (execute-do-fn (def-mapvfn `str) (pair-of "k" 1)))))

(deftest test-combine-fn
  (is (= [(pair-of "k" 6)] (execute-do-fn (def-combinefn `+) (pair-of "k" [1 2 3])))))

(deftest test-filter-fn
  (is (= [1] (execute-do-fn (def-filterfn `pos?) 1)))
  (is (empty? (execute-do-fn (def-filterfn `pos?) -1))))

(deftest test-form-to-call
  (is (= '(. :aaa parallelDo :param1 :param2 ) (form-to-call :aaa '(:parallelDo :param1 :param2 ))))
  (is (= '(. :aaa parallelDo :param1 :param2 ) (form-to-call :aaa '(:parallelDo :param1 :param2 :> var1))))
  (is (= '(. :aaa parallelDo) (form-to-call :aaa '(:parallelDo ))))
  (is (= '(. :aaa parallelDo) (form-to-call :aaa '(:parallelDo :> var2)))))
