(ns crackle.core
  (:use crackle.impl.pipeline)
  (:use crackle.impl.gen-jar))

(defn pair-of [one two]
  (org.apache.crunch.Pair/of one two))

;todo: dedup all these
(defmacro fn-mapcat [name [param] type & body]
  (let [implf (symbol (str name "-internal"))
        implf-sym `(var ~implf)]
    `(do
       (defn ~implf [~param] ~@body)
       (defn ~name []
         (fn [pcoll#] (.parallelDo pcoll# ~(str name) (crackle.fn.MapCatFnWrapper. (portable-fn ~implf-sym)) ~(type-form type)))))))

(defmacro fn-map [name [param] type & body]
  (let [implf (symbol (str name "-internal"))
        implf-sym `(var ~implf)]
    `(do
       (defn ~implf [~param] ~@body)
       (defn ~name []
         (fn [pcoll#] (.parallelDo pcoll# ~(str name) (crackle.fn.MapFnWrapper. (portable-fn ~implf-sym)) ~(type-form type)))))))

(defmacro fn-combine [name [p1 p2] & body]
  (let [implf (symbol (str name "-internal"))
        implf-sym `(var ~implf)]
    `(do
       (defn ~implf [~p1 ~p2] ~@body)
       (defn ~name []
         (fn [pcoll#] (.combineValues pcoll# (crackle.fn.CombineFnWrapper. (portable-fn #'reduce) (portable-fn ~implf-sym))))))))

(defmacro fn-mapv [name [param] vtype & body]
  (let [implf (symbol (str name "-internal"))
        implf-sym `(var ~implf)]
    `(do
       (defn ~implf [~param] ~@body)
       (defn ~name []
         (fn [pcoll#]
           (.parallelDo pcoll# ~(str name) (crackle.fn.MapValueFnWrapper. (portable-fn ~implf-sym)) (table-type-with-value pcoll# ~(type-form vtype))))))))

(defmacro fn-mapk [name [param] ktype & body]
  (let [implf (symbol (str name "-internal"))
        implf-sym `(var ~implf)]
    `(do
       (defn ~implf [~param] ~@body)
       (defn ~name []
         (fn [pcoll#]
           (.parallelDo pcoll# ~(str name) (crackle.fn.MapKeyFnWrapper. (portable-fn ~implf-sym)) (table-type-with-key pcoll# ~(type-form ktype))))))))

(defmacro fn-filter [name [param] & body]
  (let [implf (symbol (str name "-internal"))
        implf-sym `(var ~implf)]
    `(do
       (defn ~implf [~param] ~@body)
       (defn ~name []
         (fn [pcoll#]
           (.parallelDo pcoll# ~(str name) (crackle.fn.FilterFnWrapper. (portable-fn ~implf-sym))))))))

(defn count-values []
  (fn [pcoll] (.count pcoll)))

(defn collect-values []
  (fn [pcoll] (.collectValues pcoll)))

(defn group-by-key []
  (fn [pcoll] (.groupByKey pcoll)))

(defn sort-items [ascending]
  (fn [pcoll] (.sort pcoll ascending)))

(defn top [count]
  (fn [pcoll] (.top pcoll count)))

(defn bottom [count]
  (fn [pcoll] (.bottom pcoll count)))

(defn length []
  (fn [pcoll] (.length pcoll)))

(defn max-item []
  (fn [pcoll] (.max pcoll)))

(defn min-item []
  (fn [pcoll] (.min pcoll)))

(defn sample [probability]
  (fn [pcoll] (.sample pcoll probability)))

(defmacro pipeline [& body]
  (let [opts (set (filter keyword? body))
        result (first (filter vector? body))
        forms (filter list? body)
        source-fn (first forms)
        in-memory? (contains? opts :mem )
        debug? (contains? opts :debug )
        pipeline-sym (gensym "pipeline-")
        source-sym (gensym "source-")]

    `(binding [*compile-path* (get-temp-dir)]
       ~(when-not in-memory? `(.mkdir (clojure.java.io/file *compile-path*)))

       (let [~pipeline-sym (crackle.pipeline.PipelineFactory/getPipeline ~in-memory?)
             ~source-sym (~source-fn ~pipeline-sym)
            ~@(expand-pipeline-forms source-sym (rest forms))]
         ~(when debug? `(.enableDebug ~pipeline-sym))
         ~(when-not in-memory? `(setup-job-classpath ~pipeline-sym))
         (.done ~pipeline-sym)
         ~result))))

