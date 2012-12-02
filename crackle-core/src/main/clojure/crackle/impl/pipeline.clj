(ns crackle.impl.pipeline
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

(defn portable-fn [f]
  (cond
    (list? f)
    (do
      (println "inline" (pr-str f))
      (crackle.fn.PortableFnInline. (pr-str f)))

    (var? f)
    (do
      (println "var" f)
      (let [m (meta f)]
        (when-not (nil? *compile-path*) (compile (.getName (:ns m))))
        (crackle.fn.PortableFnVar. (:ns m) (:name m))))

    :else (throw (IllegalArgumentException. (str "not a var and not a list: " (pr-str f))))))

(defn type-form [type]
  (println type)
  (cond
    (= :clojure type) `(crackle.types.Clojure/anything)
    (not (vector? type)) `(. org.apache.crunch.types.writable.Writables ~(symbol (name type)))
    (= 2 (count type)) `(org.apache.crunch.types.writable.Writables/tableOf ~(type-form (first type)) ~(type-form (second type)))
    :else (throw (IllegalArgumentException. (str "unsupported type form" type)))))

(defn table-type-with-value [ptable vtype]
  (org.apache.crunch.types.writable.Writables/tableOf (.getKeyType (.getPTableType ptable)) vtype))

(defn table-type-with-key [ptable ktype]
  (org.apache.crunch.types.writable.Writables/tableOf ktype (.getValueType (.getPTableType ptable))))

