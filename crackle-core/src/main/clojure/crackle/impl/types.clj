(ns crackle.impl.types)

(defn- family-resolver [family]
  (cond
    (and (keyword? family) (= :writables family))
    `(org.apache.crunch.types.writable.WritableTypeFamily/getInstance)
    (and (keyword? family) (= :avros family))
    `(org.apache.crunch.types.avro.AvroTypeFamily/getInstance)
    :else family))

(defn- get-family-type-method [v]
  (let [v-size (count v)]
    (cond
      (empty? v) (throw (IllegalStateException. "empty type vector"))
      (= 1 v-size) (throw (IllegalStateException. "type vector of size 1"))
      (= 2 v-size) 'tableOf
      (= 3 v-size) 'triples
      (= 4 v-size) 'quads
      :else 'tuples)))

(defn class-type-resolver [type]
  (cond
    (isa? type org.apache.hadoop.io.Writable)
    (org.apache.crunch.types.writable.Writables/writables type)

    (isa? type org.apache.avro.specific.SpecificRecord)
    (org.apache.crunch.types.avro.Avros/specifics type)

    (isa? type org.apache.avro.generic.GenericRecord)
    (org.apache.crunch.types.avro.Avros/generics type)

    :else
    (throw (IllegalArgumentException. (str "unsupported type class '" type "'")))))

(defn simple-type-resolver [family type]
  (cond
    (= :clojure type) ;todo: check for writables family
    `(crackle.types.Clojure/anything)

    (keyword? type)
    `(. ~family ~(symbol (name type)))

    (vector? type)
    `(. ~family ~(get-family-type-method type) ~@(map (partial simple-type-resolver family) type))

    :else
    `(class-type-resolver ~type)))

(defn global-type-resolver [type]
  (if (map? type)
    (simple-type-resolver (family-resolver (:family type)) (:type type))
    (simple-type-resolver (family-resolver :writables) type)))

(defn table-type-with-value [ptable vtype]
  (let [table-type (.getPTableType ptable)
        family (.getFamily table-type)
        key-type (.getKeyType table-type)]
    (.tableOf family key-type vtype)))

(defn table-type-with-key [ptable ktype]
  (let [table-type (.getPTableType ptable)
        family (.getFamily table-type)
        value-type (.getValueType table-type)]
    (.tableOf family ktype value-type)))
