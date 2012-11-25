(ns crackle.impl.fn.dofn
  (:gen-class
    :extends org.apache.crunch.DoFn
    :state state
    :init init
    :constructors {[Object] []}))

(defn emitter-fn [^org.apache.crunch.Emitter emitter]
  (fn [v] (.emit emitter v)))

(defn -init [f]
  [[] f])

(defn -initialize [this]
  (require (symbol (namespace (.state this)))))

(defn -process [this input emitter]
  ((resolve (.state this)) (emitter-fn emitter) input))
