(ns crackle.impl.jar
  (:require clojure.java.shell)
  (:import [org.apache.crunch.util DistCache])
  (:import [java.io File FileOutputStream])
  (:import [java.util.jar JarOutputStream]))

(def ^:dynamic tmp-root "/tmp")

(defn- get-temp-dir []
  (File. tmp-root (str "crackletmp" (System/currentTimeMillis))))

(defn full-path [^File file]
  (.getAbsolutePath file))

(defn jar-dir [dir]
  (let [jar-file (File. (str (full-path dir) ".jar"))]
    (clojure.java.shell/sh "jar" "cf" (full-path jar-file) "-C" (full-path dir) ".")
    jar-file))

(defn setup-job-jar [pipeline]
  (let [compile-path-file (get-temp-dir)]
    (binding [*compile-path* (.getAbsolutePath compile-path-file)]
      (.mkdir compile-path-file)
      (DistCache/addJarDirToDistributedCache (.getConfiguration pipeline) (System/getProperty "crackle.lib.dir"))
      (DistCache/addJarToDistributedCache (.getConfiguration pipeline) (jar-dir compile-path-file)))))
