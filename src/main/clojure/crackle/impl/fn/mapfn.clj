(ns crackle.impl.fn.mapfn
  (:gen-class
    :extends org.apache.crunch.MapFn
    :state state
    :init init
    :constructors {[Object] []}))

(defn -init [f]
  [[] f])

(defn -initialize [this]
  (require (symbol (namespace (.state this)))))

(defn -map [this value]
  ((resolve (.state this)) value))
