(ns crackle.core
  (:use crackle.impl.debug)
  (:use crackle.impl.types)
  (:use crackle.impl.portable)
  (:use crackle.impl.gen-jar)
  (:import [org.apache.crunch PCollection PTable DoFn PGroupedTable CombineFn FilterFn]
           [org.apache.crunch.lib Aggregate Sort Sort$Order Sample PTables]))

(defn pair-of [one two]
  (org.apache.crunch.Pair/of one two))

(defmacro defn-map [ & params ]
  (generate-internal-fn crackle.fn.MapFnWrapper params))

(defmacro defn-mapcat [ & params ]
  (generate-internal-fn crackle.fn.MapCatFnWrapper params))

(defmacro defn-mapk [ & params ]
  (generate-internal-fn crackle.fn.MapKeyFnWrapper params))

(defmacro defn-mapv [ & params ]
  (generate-internal-fn crackle.fn.MapValueFnWrapper params))

(defmacro defn-filter [ & params ]
  (generate-internal-fn crackle.fn.FilterFnWrapper params))

(defmacro defn-combine [ & params ]
  (generate-internal-fn crackle.fn.CombineFnWrapper params))

(defn count! []
  (fn [^PCollection pcoll] (Aggregate/count pcoll)))

(defn collect-value! []
  (fn [^PTable pcoll] (Aggregate/collectValues pcoll)))

(defn group-by-key! []
  (fn [^PTable pcoll] (.groupByKey pcoll)))

(defn sort! [ascending?]
  (fn [^PCollection pcoll]
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

(defn keys! [limit]
  (fn [^PTable pcoll] (PTables/keys pcoll)))

(defn values! [limit]
  (fn [^PTable pcoll] (PTables/values pcoll)))

(defn parallel-do! [^DoFn do-fn ptype]
  (fn [^PCollection pcoll] (.parallelDo pcoll do-fn (global-type-resolver ptype))))

(defn combine-values! [^CombineFn combine-fn]
  (fn [^PGroupedTable pcoll] (.combineValues pcoll combine-fn)))

(defn filter! [^FilterFn filter-fn]
  (fn [^PCollection pcoll] (.filter pcoll filter-fn)))

(defn- expand-pipeline-forms [source-sym forms]
  (loop [previous-sym source-sym
         more forms
         result []]
    (if (empty? more) result
      (let [current (first more)
            to-sym (gensym)
            from-sym previous-sym]
        (recur to-sym (rest more) (concat result [to-sym (list current from-sym)]))))))

(defmacro do-pipeline [& body]
  (let [opts (set (filter keyword? body))
        result (first (filter vector? body))
        forms (filter seq? body)
        source-fn (first forms)
        in-memory? (contains? opts :mem )
        debug? (contains? opts :debug )
        pipeline-sym (gensym "pipeline-")
        source-sym (gensym "source-")
        pipeline-forms (expand-pipeline-forms source-sym (rest forms))
        last-result (first (take-last 2 pipeline-forms))]

    (binding [DEBUG-ON (or debug? DEBUG-ON)]
      (debug "forms" forms)
      (debug "result" result)
      (debug "opts" opts)
      (debug "source" source-fn)
      (debug "body" body))

    `(binding [*compile-path* (get-temp-dir)]
       ~(when-not in-memory?
          `(.mkdir (clojure.java.io/file *compile-path*)))

       (let [~pipeline-sym (crackle.pipeline.PipelineFactory/getPipeline ~in-memory?)
             ~source-sym (~source-fn ~pipeline-sym)
             ~@pipeline-forms]
         ~(when debug? `(.enableDebug ~pipeline-sym))
         ~(when-not in-memory? `(setup-job-dependencies ~pipeline-sym))
         (.done ~pipeline-sym)
         ~(if (empty? result) last-result result)))))

