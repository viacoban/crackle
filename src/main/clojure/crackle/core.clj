(ns crackle.core
  (:require crackle.impl.fn.dofn)
  (:require crackle.impl.fn.mapfn)
  (:require crackle.impl.fn.mapvfn)
  (:require crackle.impl.fn.combinefn)
  (:require crackle.impl.fn.filterfn)
  (:use crackle.impl.jar))

(defn pair-of [one two]
  (org.apache.crunch.Pair/of one two))

(defn def-dofn [f]
  (crackle.impl.fn.dofn. f))

(defn def-mapfn [f]
  (crackle.impl.fn.mapfn. f))

(defn def-mapvfn [f]
  (crackle.impl.fn.mapvfn. f))

(defn def-filterfn [f]
  (crackle.impl.fn.filterfn. f))

(defn def-combinefn [f]
  (crackle.impl.fn.combinefn. f))


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
  `(let [pipeline# (crackle.MRPipelineImpl.)]
     (do
       (.enableDebug pipeline#)
       (-> pipeline# (. read ~source) ~@(map crunch-call body))
       (setup-job-jar pipeline#)
       (.done pipeline#))))

