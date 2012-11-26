(ns crackle.impl.fn.combinefn
  (:use crackle.impl.fn.common)
  (:gen-class
    :extends org.apache.crunch.CombineFn
    :state state
    :init init
    :constructors {[Object] []}))

(defn -init [f]
  [[] f])

(defn -initialize [this]
  (load-namespace this))

(defn -process [this ^org.apache.crunch.Pair pair ^org.apache.crunch.Emitter emitter]
  ((emitter-fn emitter) (pair-of (.first pair) (reduce (as-fn this) (.second pair)))))
