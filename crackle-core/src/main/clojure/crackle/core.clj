(ns crackle.core
  (:use crackle.impl.debug)
  (:use crackle.impl.core)
  (:use crackle.impl.gen-jar))

(defn pair-of [one two]
  (org.apache.crunch.Pair/of one two))

(op-method op:count count)

(op-method op:collect-values collectValues)

(op-method op:group-by-key groupByKey)

(op-method op:sort sort ascending)

(op-method op:top top count)

(op-method op:bottom bottom count)

(op-method op:length length)

(op-method op:max max)

(op-method op:min min)

(op-method op:sample sample probability)

(op-method op:keys keys)

(op-method op:values values)

(defmacro fn-mapcat [name params type & body]
  (fn-helper name [(first params) (vec (rest params))] body
    (fn [pcoll sym args]
      `(.parallelDo ~pcoll ~(str name)
         (crackle.fn.MapCatFnWrapper. (portable-fn ~sym) (portable-args ~args)) ~(type-form type)))))

(defmacro fn-map [name params type & body]
  (fn-helper name [(first params) (vec (rest params))] body
    (fn [pcoll sym args]
      `(.parallelDo ~pcoll ~(str name)
         (crackle.fn.MapFnWrapper. (portable-fn ~sym) (portable-args ~args)) ~(type-form type)))))

(defmacro fn-combine [name params & body]
  (fn-helper name params body
    (fn [pcoll sym args]
      `(.combineValues ~pcoll
         (crackle.fn.CombineFnWrapper. (portable-fn #'reduce) (portable-fn ~sym))))))

(defmacro fn-mapv [name params vtype & body]
  (fn-helper name [(first params) (vec (rest params))] body
    (fn [pcoll sym args]
      `(.parallelDo ~pcoll ~(str name)
         (crackle.fn.MapValueFnWrapper. (portable-fn ~sym) (portable-args ~args))
         (table-type-with-value ~pcoll ~(type-form vtype))))))

(defmacro fn-mapk [name params ktype & body]
  (fn-helper name [(first params) (vec (rest params))] body
    (fn [pcoll sym args]
      `(.parallelDo ~pcoll ~(str name)
         (crackle.fn.MapKeyFnWrapper. (portable-fn ~sym) (portable-args ~args))
         (table-type-with-key ~pcoll ~(type-form ktype))))))

(defmacro fn-filter [name params & body]
  (fn-helper name [(first params) (vec (rest params))] body
    (fn [pcoll sym args]
      `(.filter ~pcoll ~(str name)
         (crackle.fn.FilterFnWrapper. (portable-fn ~sym) (portable-args ~args))))))

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
         ~result))))

