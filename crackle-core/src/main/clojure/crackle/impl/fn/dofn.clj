(ns crackle.impl.fn.dofn
  (:use crackle.impl.fn.common)
  (:gen-class
    :extends org.apache.crunch.DoFn
    :state state
    :init init
    :constructors {[Object] []}))

(defn -init [f]
  [[] f])

(defn -initialize [this]
  (load-namespace this))

(defn -process [this input emitter]
  ((as-fn this) (emitter-fn emitter) input))
