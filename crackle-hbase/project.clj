(defproject crackle/crackle-hbase "0.5.3"
  :description "HBase support for crackle-core"
  :url "https://github.com/viacoban/crackle"

  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}

  :min-lein-version "2.0.0"

  :source-paths ["src/main/clojure"]
  :test-paths ["src/test/clojure"]
  :resource-paths ["src/main/resources"]
  :java-source-paths["src/main/java"]

  :repositories [["cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos"]]

  :dependencies [[org.clojure/clojure "1.5.1"]
                 [crackle/crackle-core "0.5.3"]
                 [org.apache.crunch/crunch-hbase "0.5.0-cdh4.1.3"]
                 [org.apache.hbase/hbase "0.92.1-cdh4.1.3"]])
