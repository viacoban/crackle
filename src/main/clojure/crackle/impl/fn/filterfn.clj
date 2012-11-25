(ns crackle.impl.fn.filterfn
  (:gen-class
    :extends org.apache.crunch.FilterFn
    :state state
    :init init
    :constructors {[Object] []}))

(defn -init [f]
  [[] f])

(defn -initialize [this]
  (require (symbol (namespace (.state this)))))

(defn -accept [this value]
  ((resolve (.state this)) value))
