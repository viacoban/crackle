(ns crackle.ptype
  (:use [carbonite.api :as k])
  (:import [org.apache.crunch.types.writable Writables]))

(def registry (k/default-registry))

(def input-fn
  (proxy [org.apache.crunch.MapFn] []
    (map [input]
      (k/read-buffer registry input))))

(def output-fn
  (proxy [org.apache.crunch.MapFn] []
    (map [output]
      (let [b (k/new-buffer 1024)]
        (do
          (k/write-buffer registry b output)
          b)))))

(def ptype
  (Writables/derived (class Object) input-fn output-fn (Writables/bytes)))

(def ptabletype
  (Writables/tableOf ptype ptype))

