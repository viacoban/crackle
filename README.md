> **Clojure aphorism**:
> Clojure programmers don’t write their apps in Clojure.
> They write the language that they use to write their apps in Clojure.
>
>  _"The Joy of Clojure"_

## Crackle

A Clojure wrapper for [Apache Crunch](http://incubator.apache.org/crunch/)

## Installation

Crackle is available on [Clojars](https://clojars.org/), please report any issues [here](https://github.com/viacoban/crackle/issues).

with Leiningen:

```clj
[crackle/crackle-core "0.5.0-SNAPSHOT"]
```

with Maven:

```xml
<dependency>
 <groupId>crackle</groupId>
 <artifactId>crackle-core</artifactId>
 <version>0.5.0-SNAPSHOT</version>
</dependency>
```

## Usage

```clj
(ns crackle.example
  (:use crackle.core)
  (:require [crackle.from :as from])
  (:require [crackle.to :as to]))

;====== word count example ===============
(defn-mapcat split-words [regexp] :strings
  (fn [line] (clojure.string/split line regexp)))

(defn count-words [input-path output-path]
  (do-pipeline :debug
    (from/text-file input-path)
    (parallel-do! (split-words #"\s+"))
    (count!)
    (to/text-file output-path)))

;;====== average bytes by ip example ======
(defn-mapcat parse-line [regexp] [:strings :clojure]
  (fn [line]
    (let [[address bytes] (clojure.string/split line regexp)]
      (pair-of address [(read-string bytes) 1]))))

(defn-combine sum-bytes-and-counts []
  (fn [value1 value2]
    (mapv + value1 value2)))

(defn-mapv compute-average [] [:strings :ints]
  (fn [[bytes requests]]
    (int (/ bytes requests))))

(defn count-bytes-by-ip [input-path output-path]
  (do-pipeline
    (from/text-file input-path)
    (parallel-do! (parse-line #"\s+"))
    (group-by-key!)
    (combine-values! (sum-bytes-and-counts))
    (parallel-do! (compute-average))
    (to/text-file output-path)))

```

## License

Copyright © 2012-2013 Victor Iacoban <victor.iacoban@gmail.com>

Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.
