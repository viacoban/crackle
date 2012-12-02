(ns crackle.pipeline-test
  (:use crackle.impl.pipeline)
  (:use clojure.test))

;(deftest test-expand-method-form
;  (is (= '(. :aaa parallelDo :param1 :param2 ) (expand-method-form :aaa '(:parallelDo :param1 :param2 ))))
;  (is (= '(. :aaa parallelDo :param1 :param2 ) (expand-method-form :aaa '(:parallelDo :param1 :param2 :as var1))))
;  (is (= '(. :aaa parallelDo) (expand-method-form :aaa '(:parallelDo ))))
;  (is (= '(. :aaa parallelDo) (expand-method-form :aaa '(:parallelDo :as var2)))))

(def pipeline-sym (gensym "pipeline-"))

(def expected1 '(source-362 (.read pipeline-348 (source "/tmp/source1"))
                 step-363 (. source-362 method1)
                 step-364 (. step-363 method2)))

(def expected2 '(source-362 (.read pipeline-348 (source "/tmp/source2"))
                 var1 (. source-362 method1)
                 step-364 (. step-363 method2)))

;fail because of gensym, need to find a way to test macro
;(deftest test-expand-step-form
;  (is (= expected1 (expand-step-form pipeline-sym '(with (source "/tmp/source") (method1) (method2)))))
;  (is (= expected2 (expand-step-form pipeline-sym '(with (source "/tmp/source") (method1 :as var1) (method2))))))
