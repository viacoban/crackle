## Crackle

A Clojure wrapper around [Apache Crunch](http://incubator.apache.org/crunch/)


## Installation

Crackle is available on [Clojars](https://clojars.org/)

with Leiningen:

```clj
[crackle/crackle-core "0.2.0"]
```

with Maven:

```xml
<dependency>
 <groupId>crackle</groupId>
 <artifactId>crackle-core</artifactId>
 <version>0.2.0</version>
</dependency>
```

## Usage

Word count example with Crackle:

```clj
(ns crackle.example
  (:use crackle.core)
  (:use crackle.source)
  (:use crackle.target)
  (:require [crackle.types :as t]))

;====== word count example ===============
(defn split-words [f line]
  (doseq [word (clojure.string/split line #"\s+")] (f word)))

(defn count-words [input-path output-path]
  (mr-pipeline (from-text-file input-path)
    (:parallelDo (def-dofn `split-words) (t/strings))
    (:count)
    (:write (to-text-file output-path))))

;====== average bytes by ip example ======
(defn parse-line [line]
  (let [parts (clojure.string/split line #"\s+")]
    (pair-of (first parts) [(read-string (second parts)) 1])))

(defn sum-pairs [a b]
  [(+ (first a) (first b)) (+ (second a) (second b))])

(defn compute-average [pair]
  (int (apply / pair)))

(defn count-bytes-by-ip [input-path output-path]
  (mr-pipeline (from-text-file input-path)
    (:parallelDo (def-mapfn `parse-line) (t/table-of-binary))
    (:groupByKey)
    (:combineValues (def-combinefn `sum-pairs))
    (:parallelDo (def-mapvfn `compute-average) (t/w-table-of (t/strings) (t/wints)))
    (:write (to-text-file output-path))))

```

## License

Copyright © 2012 Victor Iacoban <victor.iacoban@gmail.com>

Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.
