(ns crackle.impl.gen-jar
  (:require [clojure.java.io :as io])
  (:use [clojure.tools.logging :only [info error debug]])
  (:import [org.apache.crunch Pipeline])
  (:import [org.apache.crunch.util DistCache])
  (:import [org.apache.commons.io IOUtils FileUtils])
  (:import [org.apache.hadoop.fs Path FileSystem])
  (:import [org.apache.hadoop.filecache DistributedCache])
  (:import [java.io File FileOutputStream FileInputStream])
  (:import [java.util.jar JarOutputStream JarEntry]))

(def ^:dynamic crackle-tmp-root "/tmp")

(def libs-dir-property "crackle.job.deps.dir")

(def ^:dynamic *cached-libs-dir*
  (str "/tmp/" (System/getProperty "user.name") "/cache/libs/"))

(defn get-temp-dir []
  (str crackle-tmp-root "/crackletmp" (System/currentTimeMillis)))

(defn next-entry-name [parent ^File file]
  (str parent (.getName file) (if (.isDirectory file) "/" "")))

(defn add-next [^File file ^String entry-name ^JarOutputStream stream]
  (let [^JarEntry entry (JarEntry. entry-name)]
    (.putNextEntry stream entry)
    (when (.isFile file)
      (with-open [source (io/input-stream file)]
        (IOUtils/copy source stream)))
    (.closeEntry stream)
    (when (.isDirectory file)
      (doseq [f (.listFiles file)]
        (add-next f (next-entry-name entry-name f) stream)))))

(defn create-jar [dir]
  (let [jar-file (io/file (str dir ".jar"))]
    (with-open [stream (JarOutputStream. (io/output-stream jar-file))]
      (add-next (io/file dir) "" stream))
    jar-file))

(defn jar-to-include? [url]
  (let [java-home (System/getProperty "java.home")
        file-path (.getFile url)]
    (cond
      (.startsWith file-path java-home) false
      (.endsWith file-path ".jar") true
      :else false)))

(defn snapshot-jar? [url]
  (.endsWith (str url) "-SNAPSHOT.jar"))

(defn find-classpath-entries []
  (loop [class-loader (.getContextClassLoader (Thread/currentThread))
         entries []]
    (if (nil? class-loader) entries
      (recur (.getParent class-loader)
        (concat entries (.getURLs class-loader))))))

(defn setup-job-from-classpath [configuration compile-path]
  (doseq [entry (find-classpath-entries)]
    (let [file-system (FileSystem/get configuration)
          entry-file (io/file (.getFile entry))]
      (cond
        (not (.exists entry-file))
        (debug "unexpected classpath entry" entry)

        (snapshot-jar? entry)
        (DistCache/addJarToDistributedCache configuration entry-file)

        (jar-to-include? entry)
        (let [src-path (Path. (.getCanonicalPath entry-file))
              dst-path (Path. (str *cached-libs-dir* (.getName entry-file)))]
          (debug "adding to distributed cache" entry)
          (if-not (.exists file-system dst-path)
            (.copyFromLocalFile file-system src-path dst-path))
          (DistributedCache/addArchiveToClassPath dst-path configuration))

        (.isDirectory entry-file)
        (FileUtils/copyDirectory entry-file compile-path)

        :else
        (debug "skipped" entry)))
    (DistCache/addJarToDistributedCache configuration (create-jar compile-path))))

(defn setup-job-dependencies [^Pipeline pipeline]
  (let [configuration (.getConfiguration pipeline)
        lib-dir (System/getProperty libs-dir-property)]
    (when-not (nil? lib-dir)
      (debug libs-dir-property lib-dir)
      (DistCache/addJarDirToDistributedCache configuration lib-dir))
    (when (nil? lib-dir)
      (debug libs-dir-property "empty. using current classpath.")
      (setup-job-from-classpath configuration (io/file *compile-path*)))))
