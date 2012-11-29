(ns crackle.core
  (:use crackle.gen-jar))

(defn pair-of [one two]
  (org.apache.crunch.Pair/of one two))

(defn- compile-symbol-ns [s]
  (when-not (nil? *compile-path*)
    (compile (symbol (namespace s)))))

(defn emitter-fn [^org.apache.crunch.Emitter emitter]
  (fn [v] (.emit emitter v)))

(defmacro defn-with-compile [name [f] & body]
  `(defn ~name [~f]
     (compile-symbol-ns ~f)
     ~@body))

(defn-with-compile def-dofn [f]
  (crackle.DoFnWrapper. `emitter-fn f))

(defn-with-compile def-mapfn [f]
  (crackle.MapFnWrapper. f))

(defn-with-compile def-mapvfn [f]
  (crackle.MapValueFnWrapper. f))

(defn-with-compile def-filterfn [f]
  (crackle.FilterFnWrapper. f))

(defn-with-compile def-combinefn [f]
  (crackle.CombineFnWrapper. `reduce f))

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
