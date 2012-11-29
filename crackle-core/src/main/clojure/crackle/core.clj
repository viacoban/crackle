(ns crackle.core
  (:use crackle.gen-jar))

(defn pair-of [one two]
  (org.apache.crunch.Pair/of one two))

(defn- compile-symbol-ns [s]
  (when-not (nil? *compile-path*)
    (compile (symbol (namespace s)))))

(defn emitter-fn [^org.apache.crunch.Emitter emitter]
  (fn [v] (.emit emitter v)))

(defn- portable-fn [f]
  (cond
    (list? f) (crackle.PortableFnInline. (pr-str f))
    (symbol? f)
    (do
      (println "symbol" f)
      (compile-symbol-ns f)
      (crackle.PortableFnSymbol. f))
    :else (throw (IllegalArgumentException. "not a symbol and not a list"))))

(defn def-dofn [f]
  (crackle.DoFnWrapper. (portable-fn f) (portable-fn `emitter-fn)))

(defn def-mapfn [f]
  (crackle.MapFnWrapper. (portable-fn f)))

(defn def-mapvfn [f]
  (crackle.MapValueFnWrapper. (portable-fn f)))

(defn def-filterfn [f]
  (crackle.FilterFnWrapper. (portable-fn f)))

(defn def-combinefn [f]
  (crackle.CombineFnWrapper. (portable-fn `reduce) (portable-fn f)))

(defn binding-symbol [form]
  (if (:> (set form)) (last form) (gensym "step-")))

(defn form-to-call [instance-name form]
  (let [call-form (if (:> (set form)) (drop-last 2 form) form)
        method-name (symbol (name (first call-form)))
        method-args (rest call-form)]
    (list* '. instance-name method-name method-args)))

(defn forms-to-calls [source forms]
  (loop [previous source
         others forms
         result []]
    (if (empty? others) result
      (let [current (first others)
            call-name (binding-symbol current)
            call (form-to-call previous current)]
        (recur call-name (rest others) (concat result [call-name call]))))))

(defmacro pipeline [& body]
  (let [opts (set (filter keyword? body))
        result (first (filter vector? body))
        forms (filter list? body)
        in-memory? (contains? opts :mem )
        debug? (contains? opts :debug )
        pipeline-sym (gensym "pipeline-")
        source-sym (gensym "source-")]

    `(binding [*compile-path* (get-temp-dir)]
       ~(when-not in-memory? `(.mkdir (clojure.java.io/file *compile-path*)))

       (let [~pipeline-sym (crackle.PipelineFactory/getPipeline ~in-memory?)
             ~source-sym (.read ~pipeline-sym ~(first forms))
             ~@(forms-to-calls source-sym (rest forms))]
         ~(when debug? `(.enableDebug ~pipeline-sym))
         ~(when-not in-memory? `(setup-job-classpath ~pipeline-sym))
         (.done ~pipeline-sym)
         ~result))))
