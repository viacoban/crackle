(ns crackle.impl.generator
  (:import [org.apache.crunch Pair Emitter])
  (:use [shady.defclass :only [defclass]]))

;(defn gen-combine-fn [combine-fn]
;  (defclass MyCombineFn []
;    :extends org.apache.crunch.CombineFn
;    (process [_ pair emitter]
;      ((emitter-fn emitter) (Pair/of (.first pair) (reduce combine-fn (.second pair)))))))
;
;(defn gen-mapv-fn [value-fn]
;  (defclass MyValueMapFn []
;    :extends org.apache.crunch.MapFn
;    (map ^org.apache.crunch.Pair [_ pair]
;      (Pair/of (.first pair) (value-fn (.second pair))))))
