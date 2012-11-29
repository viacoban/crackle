(ns crackle.core
  (:use crackle.core-impl)
  (:use crackle.gen-jar))

(defn pair-of [one two]
  (org.apache.crunch.Pair/of one two))

(defn def-dofn [f]
  (crackle.wrapperfn.DoFnWrapper. (portable-fn f) (portable-fn `emitter-fn)))

(defn def-mapfn [f]
  (crackle.wrapperfn.MapFnWrapper. (portable-fn f)))

(defn def-mapvfn [f]
  (crackle.wrapperfn.MapValueFnWrapper. (portable-fn f)))

(defn def-filterfn [f]
  (crackle.wrapperfn.FilterFnWrapper. (portable-fn f)))

(defn def-combinefn [f]
  (crackle.wrapperfn.CombineFnWrapper. (portable-fn `reduce) (portable-fn f)))

(defmacro pipeline [& body]
  (let [opts (set (filter keyword? body))
        result (first (filter vector? body))
        steps (filter step-form? body)
        in-memory? (contains? opts :mem )
        debug? (contains? opts :debug )
        pipeline-sym (gensym "pipeline-")]

    `(binding [*compile-path* (get-temp-dir)]
       ~(when-not in-memory? `(.mkdir (clojure.java.io/file *compile-path*)))

       (let [~pipeline-sym (crackle.pipeline.PipelineFactory/getPipeline ~in-memory?)
             ~@(mapcat (partial expand-step-form pipeline-sym) steps)]
         ~(when debug? `(.enableDebug ~pipeline-sym))
         ~(when-not in-memory? `(setup-job-classpath ~pipeline-sym))
         (.done ~pipeline-sym)
         ~result))))
