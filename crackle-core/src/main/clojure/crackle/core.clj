(ns crackle.core
  (:use crackle.impl.debug)
  (:use crackle.impl.types)
  (:use crackle.impl.portable)
  (:use crackle.impl.gen-jar))

(defn pair-of [one two]
  (org.apache.crunch.Pair/of one two))

(defn- fn-helper [name params body runner-body-fn]
  (let [implf (symbol (str name "-internal"))
        implf-sym `(var ~implf)
        pcoll-sym (gensym "pcoll")
        args-sym (gensym "args")]
    `(do
       (defn ~implf ~params ~@body)
       (defn ~name [& ~args-sym] (fn [~pcoll-sym] ~(runner-body-fn pcoll-sym implf-sym args-sym))))))

(defmacro fn-mapcat [name params type & body]
  (fn-helper name [(first params) (vec (rest params))] body
    (fn [pcoll sym args]
      `(.parallelDo ~pcoll ~(str name)
         (crackle.fn.MapCatFnWrapper. (pfn ~sym) (pargs ~args)) ~(global-type-resolver type)))))

(defmacro fn-map [name params type & body]
  (fn-helper name [(first params) (vec (rest params))] body
    (fn [pcoll sym args]
      `(.parallelDo ~pcoll ~(str name)
         (crackle.fn.MapFnWrapper. (pfn ~sym) (pargs ~args)) ~(global-type-resolver type)))))

(defmacro fn-combine [name params & body]
  (fn-helper name params body
    (fn [pcoll sym args]
      `(.combineValues ~pcoll
         (crackle.fn.CombineFnWrapper. (pfn #'reduce) (pfn ~sym))))))

(defmacro fn-mapv [name params vtype & body]
  (fn-helper name [(first params) (vec (rest params))] body
    (fn [pcoll sym args]
      `(.parallelDo ~pcoll ~(str name)
         (crackle.fn.MapValueFnWrapper. (pfn ~sym) (pargs ~args))
         (table-type-with-value ~pcoll ~(global-type-resolver vtype))))))

(defmacro fn-mapk [name params ktype & body]
  (fn-helper name [(first params) (vec (rest params))] body
    (fn [pcoll sym args]
      `(.parallelDo ~pcoll ~(str name)
         (crackle.fn.MapKeyFnWrapper. (pfn ~sym) (pargs ~args))
         (table-type-with-key ~pcoll ~(global-type-resolver ktype))))))

(defmacro fn-filter [name params & body]
  (fn-helper name [(first params) (vec (rest params))] body
    (fn [pcoll sym args]
      `(.filter ~pcoll ~(str name)
         (crackle.fn.FilterFnWrapper. (pfn ~sym) (pargs ~args))))))

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

(defmacro do-pipeline [& body]
  (let [opts (set (filter keyword? body))
        result (first (filter vector? body))
        forms (filter seq? body)
        source-fn (first forms)
        in-memory? (contains? opts :mem )
        debug? (contains? opts :debug )
        pipeline-sym (gensym "pipeline-")
        source-sym (gensym "source-")]

    (binding [DEBUG-ON (or debug? DEBUG-ON)]
      (debug "forms" forms)
      (debug "result" result)
      (debug "opts" opts)
      (debug "source" source-fn)
      (debug "body" body))

    `(binding [*compile-path* (get-temp-dir)]
       ~(when-not in-memory? `(.mkdir (clojure.java.io/file *compile-path*)))

       (let [~pipeline-sym (crackle.pipeline.PipelineFactory/getPipeline ~in-memory?)
             ~source-sym (~source-fn ~pipeline-sym)
            ~@(expand-pipeline-forms source-sym (rest forms))]
         ~(when debug? `(.enableDebug ~pipeline-sym))
         ~(when-not in-memory? `(setup-job-classpath ~pipeline-sym))
         (.done ~pipeline-sym)
         ~(if (= 1 (count result)) (first result) result)))))

