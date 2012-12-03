(ns crackle.impl.core
  (:import [org.apache.crunch.types.writable Writables]))

(defn expand-pipeline-forms [source-sym forms]
  (loop [previous-sym source-sym
         more forms
         result []]
    (if (empty? more) result
      (let [current (first more)
            call (take-while #(not (keyword? %)) current)
            opts (apply hash-map (drop-while #(not (keyword? %)) current))
            to-sym (get opts :as (gensym))
            from-sym (get opts :with previous-sym)]
        (recur to-sym (rest more) (concat result [to-sym (list call from-sym)]))))))

(defn portable-args [args]
  (crackle.fn.PortableFnArgs/getInstance args))

(defn portable-fn [f]
  (cond
    (list? f) (crackle.fn.PortableFnInline. (pr-str f))
    (var? f)
    (do
      (let [m (meta f)]
        (when-not (nil? *compile-path*) (compile (.getName (:ns m))))
        (crackle.fn.PortableFnVar. (:ns m) (:name m))))

    :else (throw (IllegalArgumentException. (str "not a var and not a list: " (pr-str f))))))

(defn type-form [type]
  (cond
    (= :clojure type) `(crackle.types.Clojure/anything)
    (not (vector? type)) `(. org.apache.crunch.types.writable.Writables ~(symbol (name type)))
    (= 2 (count type)) `(org.apache.crunch.types.writable.Writables/tableOf ~(type-form (first type)) ~(type-form (second type)))
    :else (throw (IllegalArgumentException. (str "unsupported type form" type)))))

(defn table-type-with-value [ptable vtype]
  (org.apache.crunch.types.writable.Writables/tableOf (.getKeyType (.getPTableType ptable)) vtype))

(defn table-type-with-key [ptable ktype]
  (org.apache.crunch.types.writable.Writables/tableOf ktype (.getValueType (.getPTableType ptable))))

(defn fn-helper [name params body runner-body-fn]
  (let [implf (symbol (str name "-internal"))
        implf-sym `(var ~implf)
        pcoll-sym (gensym "pcoll")
        args-sym (gensym "args")]
    `(do
       (defn ~implf ~params ~@body)
       (defn ~name [& ~args-sym] (fn [~pcoll-sym] ~(runner-body-fn pcoll-sym implf-sym args-sym))))))
