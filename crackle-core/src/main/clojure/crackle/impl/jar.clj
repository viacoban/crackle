(ns crackle.impl.jar
  (:require clojure.java.shell)
  (:import [org.apache.crunch.util DistCache])
  (:import [java.io File FileOutputStream])
  (:import [java.util.jar JarOutputStream]))

(def ^:dynamic crackle-tmp-root "/tmp")

(defn get-temp-dir []
  (str crackle-tmp-root "/crackletmp" (System/currentTimeMillis)))

(defn- full-path [^File file]
  (.getAbsolutePath file))

(defn- jar-dir [dir]
  (let [jar-file (File. (str dir ".jar"))]
    (clojure.java.shell/sh "jar" "cf" (full-path jar-file) "-C" dir ".")
    jar-file))

(defn setup-job-classpath [pipeline]
  (let [configuration (.getConfiguration pipeline)]
    (DistCache/addJarToDistributedCache (.getConfiguration pipeline) (jar-dir *compile-path*))
    (DistCache/addJarDirToDistributedCache configuration (System/getProperty "crackle.lib.dir"))))
