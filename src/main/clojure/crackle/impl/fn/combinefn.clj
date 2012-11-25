(ns crackle.impl.fn.combinefn
  (:gen-class
    :extends org.apache.crunch.CombineFn
    :state state
    :init init
    :constructors {[Object] []}))

(defn -init [f]
  [[] f])

(defn -initialize [this]
  (require (symbol (namespace (.state this)))))

(defn -process [this ^org.apache.crunch.Pair pair ^org.apache.crunch.Emitter emitter]
  (.emit emitter (org.apache.crunch.Pair/of (.first pair) (reduce (resolve (.state this)) (.second pair)))))
