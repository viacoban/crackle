(ns crackle.core
  (:use crackle.generator)
  (:use [crackle.ptype :only [ptype ptabletype]]))

(defn pair-of [one two]
  (org.apache.crunch.Pair/of one two))

(def simple-ptype ptype)

(def table-ptype ptabletype)

(defn from-txt [path]
  (org.apache.crunch.io.From/textFile path))

(defn to-txt [path]
  (org.apache.crunch.io.To/textFile path))


(defn do-fn [f]
  (.newInstance (gen-do-fn f)))

(defn combine-fn [f]
  (.newInstance (gen-combine-fn f)))

(defn map-fn [f]
  (.newInstance (gen-map-fn f)))

(defn mapv-fn [f]
  (.newInstance (gen-mapv-fn f)))

(defn- get-method-symbol [call]
  (symbol (name (first call))))

(defn- get-method-args [call]
  (rest call))

(defn- crunch-call [call]
  (concat (list '. (get-method-symbol call)) (get-method-args call)))

(defmacro mem-pipeline [source & body]
  `(let [pipeline# (org.apache.crunch.impl.mem.MemPipeline/getInstance)]
     (do
       (-> pipeline#
         (. read ~source)
         ~@(map crunch-call body))
       (.done pipeline#))))

