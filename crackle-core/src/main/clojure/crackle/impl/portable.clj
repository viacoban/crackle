(ns crackle.impl.portable)

(defn pargs [args]
  (crackle.fn.PortableFnArgs/getInstance args))

(defn pfn [f]
  (cond
    (list? f)
    (crackle.fn.PortableFnInline. (pr-str f))
    (var? f)
    (let [m (meta f)]
      (when-not (nil? *compile-path*) (compile (.getName (:ns m))))
      (crackle.fn.PortableFnVar. (:ns m) (:name m)))
    :else
    (throw (IllegalArgumentException. (str "not a var and not a list: " (pr-str f))))))

