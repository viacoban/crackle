(ns crackle.generator
  (:import [org.apache.crunch Pair])
  (:use [shady.defclass :only [defclass]]))

(defn- emitter-fn [emitter]
  (fn [o] (.emit emitter o)))

(defn gen-do-fn [input-fn]
  (defclass MyDoFn []
    :extends org.apache.crunch.DoFn
    (process [_ input emitter]
      (let [output (input-fn input)
            fn-e (emitter-fn emitter)]
        (if (seq? output) (map fn-e output) (fn-e output))))))

(defn gen-combine-fn [combine-fn]
  (defclass MyCombineFn []
    :extends org.apache.crunch.CombineFn
    (process [_ pair emitter]
      ((emitter-fn emitter) (Pair/of (.first pair) (reduce combine-fn (.second pair)))))))

(defn gen-map-fn [map-fn]
  (defclass MyEntryMapFn []
    :extends org.apache.crunch.MapFn
    (map ^clojure.lang.Obj [_ input]
      (map-fn input))))

(defn gen-mapv-fn [value-fn]
  (defclass MyValueMapFn []
    :extends org.apache.crunch.MapFn
    (map ^org.apache.crunch.Pair [_ pair]
      (Pair/of (.first pair) (value-fn (.second pair))))))

(defn gen-filter-fn [accept-fn]
  (defclass MyFilterFn []
    :extends org.apache.crunch.FilterFn
    (accept ^Boolean [_ input]
      (accept-fn input))))

