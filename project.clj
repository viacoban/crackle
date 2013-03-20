(defproject crackle "0.5.3"
  :description "Clojure wrapper for Apache Crunch"
  :url "https://github.com/viacoban/crackle"

  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}

  :min-lein-version "2.0.0"

  :profiles {:build {:dependencies
                     [[crackle/crackle-core "0.5.3"]
                      [crackle/crackle-hbase "0.5.3"]
                      [crackle/crackle-example "0.5.3"]]}}

  :plugins [[lein-sub "0.2.4"]
            [lein-clojars "0.9.1"]]

  :sub ["crackle-core" "crackle-hbase" "crackle-example"])
