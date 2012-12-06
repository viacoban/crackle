(ns crackle.impl.debug)

(def ^:dynamic DEBUG-ON false)

(defn debug [& args]
  (when DEBUG-ON (apply println args)))
