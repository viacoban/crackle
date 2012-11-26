(ns crackle.impl.fn.filterfn
  (:use crackle.impl.fn.common)
  (:gen-class
    :extends org.apache.crunch.FilterFn
    :state state
    :init init
    :constructors {[Object] []}))

(defn -init [f]
  [[] f])

(defn -initialize [this]
  (load-namespace this))

(defn -accept [this value]
  ((as-fn this) value))
