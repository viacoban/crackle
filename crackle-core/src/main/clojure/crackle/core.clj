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

(defn- get-method-symbol [call]
  (symbol (name (first call))))

(defn- get-method-args [call]
  (rest call))

(defn- crunch-call [call]
  (concat (list '. (get-method-symbol call)) (get-method-args call)))

(defmacro mem-pipeline [source & body]
  `(let [pipeline# (org.apache.crunch.impl.mem.MemPipeline/getInstance)]
     (do
       (.enableDebug pipeline#)
       (-> pipeline# (. read ~source) ~@(map crunch-call body))
       (.done pipeline#))))

(defmacro mr-pipeline [source & body]
  `(let [pipeline# (org.apache.crunch.impl.mr.MRPipeline. crackle.PortableFn)
         pipeline-dir# ~(get-temp-dir)]
     (binding [*compile-path* pipeline-dir#]
       (do
         (.mkdir (clojure.java.io/file pipeline-dir#))
         (.enableDebug pipeline#)
         (-> pipeline# (. read ~source) ~@(map crunch-call body))
         (setup-job-classpath pipeline#)
         (.done pipeline#)))))

