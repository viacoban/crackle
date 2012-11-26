(ns crackle.impl.fn.common)

(defn pair-of [one two]
  (org.apache.crunch.Pair/of one two))

(defn emitter-fn [^org.apache.crunch.Emitter emitter]
  (fn [v] (.emit emitter v)))

(defn load-namespace [this]
  (require (symbol (namespace (.state this)))))

(defn as-fn [this]
  (resolve (.state this)))
