(defproject crackle "0.5.4"
  :description "Clojure wrapper for Apache Crunch"
  :url "https://github.com/viacoban/crackle"

  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}

  :min-lein-version "2.0.0"

  :plugins [[lein-sub "0.2.4"]]

  :sub ["crackle-core" "crackle-hbase" "crackle-example"])
