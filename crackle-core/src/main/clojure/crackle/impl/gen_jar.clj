(ns crackle.impl.gen-jar
  (:import [org.apache.crunch Pipeline])
  (:import [org.apache.crunch.util DistCache])
  (:import [org.apache.commons.io IOUtils])
  (:import [java.io File FileOutputStream FileInputStream])
  (:import [java.util.jar JarOutputStream JarEntry]))

(def ^:dynamic crackle-tmp-root "/tmp")

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

(defn setup-job-classpath [^Pipeline pipeline]
  (let [configuration (.getConfiguration pipeline)
        ^File jar-file (jar-dir *compile-path*)]
    (DistCache/addJarToDistributedCache configuration jar-file)
    (DistCache/addJarDirToDistributedCache configuration (System/getProperty "crackle.lib.dir"))))
