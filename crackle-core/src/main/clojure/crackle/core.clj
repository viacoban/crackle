(ns crackle.core
  (:use crackle.impl.types)
  (:use crackle.impl.portable)
  (:use crackle.impl.gen-jar)
  (:use [clojure.tools.logging :only [info error debug]])
  (:import [org.apache.crunch PCollection PTable DoFn PGroupedTable CombineFn FilterFn]
           [org.apache.crunch.lib Aggregate Sort Sort$Order Sample PTables]))

(defn pair-of [one two]
  (org.apache.crunch.Pair/of one two))

(defmacro defn-map [ name args result-type [_ [item] impl-body] ]
  (generate-internal-fn crackle.fn.MapFnWrapper name result-type [item] args impl-body))

(defmacro defn-mapcat [ name args result-type [_ [item] impl-body] ]
  (generate-internal-fn crackle.fn.MapCatFnWrapper name result-type [item] args impl-body))

(defmacro defn-mapk [ name args result-type [_ [key] impl-body] ]
  (generate-internal-fn crackle.fn.MapKeyFnWrapper name result-type [key] args impl-body))

(defmacro defn-mapv [ name args result-type [_ [value] impl-body] ]
  (generate-internal-fn crackle.fn.MapValueFnWrapper name result-type [value] args impl-body))

(defmacro defn-filter [ name args [_ [item] impl-body] ]
  (generate-internal-fn crackle.fn.FilterFnWrapper name nil [item] args impl-body))

(defmacro defn-combine [ name args [_ [value1 value2] impl-body] ]
  (generate-internal-fn crackle.fn.CombineFnWrapper name nil [value1 value2] nil impl-body))

(defn count! []
  (fn [^PCollection pcoll] (Aggregate/count pcoll)))

(defn collect-value! []
  (fn [^PTable pcoll] (Aggregate/collectValues pcoll)))

(defn group-by-key! []
  (fn [^PTable pcoll] (.groupByKey pcoll)))

(defn sort! [ascending?]
  (fn [^PCollection pcoll]
    (Sort/sort pcoll (if ascending? (Sort$Order/ASCENDING) (Sort$Order/DESCENDING)))))

(defn sort-by-key! [ascending?]
  (fn [^PTable pcoll]
    (Sort/sort pcoll (if ascending? (Sort$Order/ASCENDING) (Sort$Order/DESCENDING)))))

(defn top! [limit]
  (fn [^PTable pcoll] (Aggregate/top pcoll limit true)))

(defn bottom! [limit]
  (fn [^PTable pcoll] (Aggregate/top pcoll limit false)))

(defn length! []
  (fn [^PCollection pcoll] (Aggregate/length pcoll)))

(defn max! []
  (fn [^PCollection pcoll] (Aggregate/max pcoll)))

(defn min! []
  (fn [^PCollection pcoll] (Aggregate/min pcoll)))

(defn sample! [acceptance-probability]
  (fn [^PCollection pcoll] (Sample/sample pcoll acceptance-probability)))

(defn keys! []
  (fn [^PTable pcoll] (PTables/keys pcoll)))

(defn values! []
  (fn [^PTable pcoll] (PTables/values pcoll)))

(defn parallel-do! [do-fn]
  {:pre [(:name do-fn)
         (:result-type do-fn)
         (isa? (class (:instance do-fn)) org.apache.crunch.DoFn)]}
  (fn [pcoll]
    (.parallelDo pcoll (:name do-fn) (:instance do-fn)
      (eval (global-type-resolver (:result-type do-fn))))))

(defn combine-values! [combine-fn]
  {:pre [(isa? (class (:instance combine-fn)) CombineFn)]}
  (fn [^PGroupedTable pcoll]
    (.combineValues pcoll (:instance combine-fn))))

(defn filter! [filter-fn]
  {:pre [(:name filter-fn) (isa? (class (:instance filter-fn)) FilterFn)]}
  (fn [^PCollection pcoll]
    (.filter pcoll (:name filter-fn) (:instance filter-fn))))

(defn by! [map-fn]
  {:pre [(:name map-fn)
         (:result-type map-fn)
         (isa? (class (:instance map-fn)) org.apache.crunch.MapFn)]}
  (fn [^PCollection pcoll]
    (.by pcoll (:name map-fn) (:instance map-fn)
      (eval (global-type-resolver (:result-type map-fn))))))

(defn- expand-pipeline-forms [source-sym forms]
  (loop [previous-sym source-sym
         more forms
         result []]
    (if (empty? more) result
      (let [current (first more)
            call (take-while #(not (keyword? %)) current)
            opts (apply hash-map (drop-while #(not (keyword? %)) current))
            to-sym (get opts :as (gensym))
            from-sym (get opts :with previous-sym)]
        (recur to-sym (rest more) (concat result [to-sym (list call from-sym)]))))))

(defmacro when* [cond & forms]
  `(if-not ~cond identity
     (comp ~@(reverse forms))))

(defn materialize-seq [pcoll]
  (try
    (->> pcoll
      (.materialize)
      (seq))
    (catch Exception ex
      (error "Error materializing " pcoll ex))))

(defmacro do-pipeline [& body]
  (let [opts (set (filter keyword? body))
        result (first (filter vector? body))
        forms (filter seq? body)
        source-fn (first forms)
        in-memory? (contains? opts :mem )
        pipeline-sym (gensym "pipeline-")
        source-sym (gensym "source-")
        pipeline-forms (expand-pipeline-forms source-sym (rest forms))
        last-result (first (take-last 2 pipeline-forms))]

    (debug "forms" forms)
    (debug "result" result)
    (debug "opts" opts)
    (debug "source" source-fn)
    (debug "body" body)

    `(binding [*compile-path* (get-temp-dir)]
       ~(when-not in-memory?
          `(.mkdir (clojure.java.io/file *compile-path*)))

       (let [~pipeline-sym (crackle.pipeline.PipelineFactory/getPipeline ~in-memory?)
             ~source-sym (~source-fn ~pipeline-sym)
             ~@pipeline-forms]
         ~(when-not in-memory? `(setup-job-dependencies ~pipeline-sym))
         (.done ~pipeline-sym)
         ~(if (empty? result) last-result result)))))

