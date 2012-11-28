## Crackle

A Clojure wrapper around [Apache Crunch](http://incubator.apache.org/crunch/)

**Attention**: This is pre-alpha quality code, things might change or stop working!


## Installation

Crackle is available on [Clojars](https://clojars.org/)

with Leiningen:

```clj
[crackle/crackle-core "0.3.0-SNAPSHOT"]
```

with Maven:

```xml
<dependency>
 <groupId>crackle</groupId>
 <artifactId>crackle-core</artifactId>
 <version>0.3.0-SNAPSHOT</version>
</dependency>
```

## Usage

Word count example with Crackle:

```clj
(ns crackle.example
  (:import [crackle.types Clojure])
  (:import [org.apache.crunch.io From To])
  (:import [org.apache.crunch.types.writable Writables])
  (:use crackle.core))

;====== word count example ===============
(defn split-words [f line]
  (doseq [word (clojure.string/split line #"\s+")] (f word)))

(defn count-words [input-path output-path]
  (mr-pipeline (From/textFile input-path)
    (parallelDo (def-dofn `split-words) (Writables/strings))
    (count)
    (write (To/textFile output-path))))

;====== average bytes by ip example ======
(defn parse-line [line]
  (let [parts (clojure.string/split line #"\s+")]
    (pair-of (first parts) [(read-string (second parts)) 1])))

(defn sum-pairs [a b]
  [(+ (first a) (first b)) (+ (second a) (second b))])

(defn compute-average [pair]
  (int (apply / pair)))

(defn count-bytes-by-ip [input-path output-path]
  (mr-pipeline (From/textFile input-path)
    (parallelDo (def-mapfn `parse-line) (Clojure/tableOf))
    (groupByKey)
    (combineValues (def-combinefn `sum-pairs))
    (parallelDo (def-mapvfn `compute-average) (Writables/tableOf (Writables/strings) (Writables/ints)))
    (write (To/textFile output-path))))

```

## License

Copyright © 2012 Victor Iacoban <victor.iacoban@gmail.com>

Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.
