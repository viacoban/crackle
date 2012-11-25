(ns crackle.impl.fn.mapvfn
  (:gen-class
    :extends org.apache.crunch.MapFn
    :state state
    :init init
    :constructors {[Object] []}))

(defn -init [f]
  [[] f])

(defn -initialize [this]
  (require (symbol (namespace (.state this)))))

(defn -map [this pair]
  (org.apache.crunch.Pair/of (.first pair) ((resolve (.state this)) (.second pair))))
