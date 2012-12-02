(ns crackle.hbase.to
  (:import [org.apache.crunch.io.hbase ToHBase]))

(defn table [^String table]
  (fn [pcoll]
    (.write pcoll (ToHBase/table table))))
