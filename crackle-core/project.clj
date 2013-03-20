(defproject crackle/crackle-core "0.5.3"
  :description "Clojure wrapper for Apache Crunch"
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
                 [org.clojure/tools.logging "0.2.6"]
                 [org.apache.crunch/crunch "0.5.0-cdh4.1.3"]
                 [org.apache.hadoop/hadoop-client "2.0.0-mr1-cdh4.1.3"]
                 [com.twitter/carbonite "1.3.1"]
                 [commons-lang/commons-lang "2.4"]
                 [commons-io/commons-io "2.4"]
                 [com.google.guava/guava "11.0.2"]])
