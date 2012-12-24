(ns crackle.impl.gen-jar
  (:use crackle.impl.debug)
  (:import [org.apache.crunch Pipeline])
  (:import [org.apache.crunch.util DistCache])
  (:import [org.apache.commons.io IOUtils])
  (:import [java.io File FileOutputStream FileInputStream])
  (:import [java.util.jar JarOutputStream JarEntry]))

(def ^:dynamic crackle-tmp-root "/tmp")
(def libs-dir-property "crackle.job.deps.dir")

(defn get-temp-dir []
  (str crackle-tmp-root "/crackletmp" (System/currentTimeMillis)))

(defn get-jar-entry-name [parent ^File file]
  (str parent (.getName file) (if (.isDirectory file) "/" "")))

(defn add-file [^File file ^String entry-name ^JarOutputStream stream]
  (let [^JarEntry entry (JarEntry. entry-name)]
    (.putNextEntry stream entry)
    (when (.isFile file) (IOUtils/copy (FileInputStream. file) stream))
    (.closeEntry stream)

    (when (.isDirectory file)
      (doseq [f (.listFiles file)]
        (add-file f (get-jar-entry-name entry-name f) stream)))))

(defn jar-dir [^String dir]
  (let [jar-file (File. (str dir ".jar"))]
    (with-open [stream (JarOutputStream. (FileOutputStream. jar-file))]
      (add-file (File. dir) "" stream))
    jar-file))

(defn include-jar? [url]
  (let [java-home (System/getProperty "java.home")
        file-path (.getFile url)]
    (cond
      (.startsWith file-path java-home) false
      (.endsWith file-path ".jar") true
      :else false)))

(defn find-classpath-jars []
  (loop [class-loader (.getContextClassLoader (Thread/currentThread))
         jars []]
    (if (nil? class-loader) jars
      (recur (.getParent class-loader) (concat jars (filter include-jar? (.getURLs class-loader)))))))

(defn setup-job-classpath [^Pipeline pipeline]
  (let [configuration (.getConfiguration pipeline)
        ^File jar-file (jar-dir *compile-path*)
        lib-dir (System/getProperty libs-dir-property)]
    (debug libs-dir-property lib-dir)
    (debug "job jar" jar-file)
    (DistCache/addJarToDistributedCache configuration jar-file)
    (if (nil? lib-dir)
      (doseq [jar-url (find-classpath-jars)]
        (debug jar-url)
        (DistCache/addJarToDistributedCache configuration (.getFile jar-url)))
      (DistCache/addJarDirToDistributedCache configuration lib-dir))))
