(ns crackle.core-impl)

(defn binding-symbol [form]
  (if (:as (set form)) (last form) (gensym "step-")))

(defn expand-method-form [instance-name form]
  (let [call-form (if (:as (set form)) (drop-last 2 form) form)
        method-name (symbol (name (first call-form)))
        method-args (rest call-form)]
    (list* '. instance-name method-name method-args)))

(defn expand-step-form [pipeline form]
  (let [source (second form)
        method-forms (nnext form)
        first-sym (gensym "source-")]
    (loop [previous-sym (if (list? source) first-sym source)
           more method-forms
           result (if (list? source) [first-sym `(.read ~pipeline ~source)] first-sym)]
      (if (empty? more) result
        (let [method-form (first more)
              binding-sym (binding-symbol method-form)
              method-call (expand-method-form previous-sym method-form)]
          (recur binding-sym (rest more) (concat result [binding-sym method-call])))))))

(defn step-form? [form]
  (and (list? form) (= "with" (name (first form)))))

(defn compile-symbol-ns [s]
  (when-not (nil? *compile-path*)
    (compile (symbol (namespace s)))))

(defn emitter-fn [^org.apache.crunch.Emitter emitter]
  (fn [v] (.emit emitter v)))

(defn portable-fn [f]
  (cond
    (list? f)
    (crackle.wrapperfn.PortableFnInline. (pr-str f))

    (symbol? f)
    (do
      (println "symbol" f)
      (compile-symbol-ns f)
      (crackle.wrapperfn.PortableFnSymbol. f))

    :else (throw (IllegalArgumentException. "not a symbol and not a list"))))
