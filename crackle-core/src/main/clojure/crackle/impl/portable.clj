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

(defn generate-internal-fn [wrapper-class fn-name result-type primary-args extra-args impl-body]
  (let [internal-fn-name# (symbol (str fn-name "-internal"))
        internal-fn-name-symbol# `(var ~internal-fn-name#)
        args# (conj primary-args extra-args)]
    `(do
       (defn ~internal-fn-name# ~args# ~impl-body)
       (defn ~fn-name ~extra-args
         {:name ~(str fn-name)
          :result-type ~result-type
          :instance (new ~wrapper-class (pfn ~internal-fn-name-symbol#) (pargs ~extra-args))}))))

