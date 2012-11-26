(ns crackle.impl.fn.mapfn
  (:use crackle.impl.fn.common)
  (:gen-class
    :extends org.apache.crunch.MapFn
    :state state
    :init init
    :constructors {[Object] []}))

(defn -init [f]
  [[] f])

(defn -initialize [this]
  (load-namespace this))

(defn -map [this value]
  ((as-fn this) value))
