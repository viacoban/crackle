(ns crackle.core
  (:use [shady.defclass :only [defclass]]))

(defn pair-of [one two]
  (org.apache.crunch.Pair/of one two))

(defn- gen-do-fn [input-fn]
  (defclass MyDoFn [] :extends org.apache.crunch.DoFn
    (process [_ input emitter]
      (doseq [o (input-fn input)]
        (.emit emitter o)))))

(defn- gen-combine-fn [combine-fn]
  (defclass MyCombineFn [] :extends org.apache.crunch.CombineFn
    (process [_ pair emitter]
      (.emit emitter (pair-of (:first pair) (reduce combine-fn (:second pair)))))))

(defn- gen-map-value-fn [value-fn]
  (defclass MyValueMapFn [] :extends org.apache.crunch.MapFn
    (map ^org.apache.crunch.Pair [_ pair]
      (pair-of (:first pair) (value-fn (:second pair))))))

(defn- gen-map-entry-fn [map-fn]
  (defclass MyEntryMapFn [] :extends org.apache.crunch.MapFn
    (map ^clojure.lang.Obj [_ input] (map-fn input))))

(defn- gen-filter-fn [accept-fn]
  (defclass MyFilterFn [] :extends org.apache.crunch.FilterFn
    (accept ^Boolean [_ input] (accept-fn input))))

(defmacro mem-pipeline [source target & body]
  `(let [pipeline# (org.apache.crunch.impl.mem.MemPipeline/getInstance)]
     (do
       (-> (.read pipeline# ~source)
         ~@body
         (.write ~target))
       (.done pipeline#))))

(defn =each-to-seq [pcoll input-fn]
  (.parallelDo pcoll (.newInstance (gen-do-fn input-fn)) (crackle.ClojureTypes/getType)))

(defn =each [pcoll map-fn]
  (.parallelDo pcoll (.newInstance (gen-map-entry-fn map-fn)) (crackle.ClojureTypes/getType)))

(defn =count [pcoll]
  (.count pcoll))

(defn =combine-values [pcoll combine-fn]
  (.combineValues (.newInstance (gen-combine-fn combine-fn))))

(defn =by [pcoll key-fn]
  (.by (.newInstance (gen-map-entry-fn key-fn)) (crackle.ClojureTypes/getType)))

(defn =map-value [pcoll value-fn]
  (.parallelDo (.newInstance (gen-map-value-fn value-fn)) (crackle.ClojureTypes/getType)))

(defn =filter [pcoll filter-fn]
  (.filter (.newInstance (gen-filter-fn filter-fn))))

(defn =sort [pcoll ascending?]
  (.sort pcoll ascending?))

(defn =sample [pcoll acceptance-probability]
  (.sample pcoll acceptance-probability))

(defn =group-by-key [pcoll]
  (.groupByKey pcoll))

(defn =collect-values [pcoll]
  (.collectValues pcoll))

(defn =top [pcoll n]
  (.top pcoll n))

(defn =bottom [pcoll n]
  (.bottom pcoll n))

(defn =keys [pcoll]
  (.keys pcoll))

(defn =values [pcoll]
  (.values pcoll))

(defn from-txt [path]
  (org.apache.crunch.io.From/textFile path))

(defn to-txt [path]
  (org.apache.crunch.io.To/textFile path))
