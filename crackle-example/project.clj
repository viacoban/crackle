(defproject crackle/crackle-example "0.5.4"
  :description "Some examples for crackle"
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

  :dependencies [[org.clojure/clojure "1.5.1"]
                 [crackle/crackle-core "0.5.4"]])
