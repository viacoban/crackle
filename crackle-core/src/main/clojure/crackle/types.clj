(ns crackle.types
  (:import [crackle BinaryTypes])
  (:import [org.apache.crunch.types.writable Writables]))

(defn nulls []
  (Writables/nulls))

(defn strings []
  (Writables/strings))

;(defn longs []
;  (Writables/longs))
;
(defn wints []
  (Writables/ints))
;
;(defn floats []
;  (Writables/floats))
;
;(defn doubles []
;  (Writables/doubles))
;
;(defn booleans []
;  (Writables/booleans))
;
;(defn bytes []
;  (Writables/bytes))
;
;(defn writables [clazz]
;  (Writables/writables clazz))

(defn w-table-of [type1 type2]
  (Writables/tableOf type1 type2))


(defn binary []
  (BinaryTypes/anything))

(defn table-of-binary []
  (Writables/tableOf (binary) (binary)))
