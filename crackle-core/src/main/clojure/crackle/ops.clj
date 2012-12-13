(ns crackle.ops
  (:refer-clojure :exclude [count sort max min keys]))

(defmacro op-method [name method & args]
  `(defn ~name [~@args]
     (fn [pcoll#] (. pcoll# ~method ~@args))))

(op-method count count)

(op-method collect-values collectValues)

(op-method group-by-key groupByKey)

(op-method sort sort ascending)

(op-method top top count)

(op-method bottom bottom count)

(op-method length length)

(op-method max max)

(op-method min min)

(op-method sample sample probability)

(op-method keys keys)

(op-method values values)
