(ns crackle.hbase.from
  (:import [org.apache.crunch.io.hbase FromHBase]))

(defn table
  ([^String table]
    (fn [pipeline] (.read pipeline (FromHBase/table table))))

  ([^String table scan]
    (fn [pipeline] (.read pipeline (FromHBase/table table scan)))))
