(ns crackle.impl.fn.mapvfn
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

(defn -map [this ^org.apache.crunch.Pair pair]
  (pair-of (.first pair) ((as-fn this) (.second pair))))
