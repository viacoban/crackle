## Crackle

A Clojure wrapper around [Apache Crunch](http://incubator.apache.org/crunch/)


## Installation

Crackle is available on [Clojars](https://clojars.org/)

with Leiningen:

```clj
[crackle "0.2.0"]
```

with Maven:

```xml
<dependency>
 <groupId>crackle</groupId>
 <artifactId>crackle</artifactId>
 <version>0.2.0</version>
</dependency>
```

## Usage

Word count example with Crackle:

```clj
;====== word count example ===============
(defn split-words [line]
  (clojure.string/split line #"\s+"))

(defn count-words [input-path output-path]
  (mem-pipeline (from-txt input-path)
    (parallelDo (do-fn split-words) simple-ptype)
    (count)
    (write (to-txt output-path))))

;====== average bytes by ip example ======
(defn parse-line [line]
  (let [parts (split-words line)]
    (pair-of (first parts) [(read-string (second parts)) 1])))

(defn sum-pairs [a b]
  [(+ (first a) (first b)) (+ (second a) (second b))])

(defn compute-average [sum-and-count-pair]
  (apply / sum-and-count-pair))

(defn count-bytes-by-ip [input-path output-path]
  (mem-pipeline (from-txt input-path)
    (parallelDo (do-fn parse-line) table-ptype)
    (groupByKey)
    (combineValues (combine-fn sum-pairs))
    (parallelDo (mapv-fn compute-average) table-ptype)
    (write (to-txt output-path))))

```

## License

Copyright © 2012 Victor Iacoban <victor.iacoban@gmail.com>

Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.
