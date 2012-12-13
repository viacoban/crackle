> **Clojure aphorism**:
> Clojure programmers don’t write their apps in Clojure.
> They write the language that they use to write their apps in Clojure.
>
>  _"The Joy of Clojure"_

## Crackle

A Clojure wrapper for [Apache Crunch](http://incubator.apache.org/crunch/)

## Installation

**This is development quality code, things might change or stop working**!

Crackle is available on [Clojars](https://clojars.org/)

with Leiningen:

```clj
[crackle/crackle-core "0.4.0-SNAPSHOT"]
```

with Maven:

```xml
<dependency>
 <groupId>crackle</groupId>
 <artifactId>crackle-core</artifactId>
 <version>0.4.0-SNAPSHOT</version>
</dependency>
```

## Usage

```clj
(ns crackle.example
  (:use crackle.core)
  (:require [crackle.from :as from])
  (:require [crackle.to :as to])
  (:require [crackle.ops :as op]))

;====== word count example ===============
(fn-mapcat split-words [line re] :strings
  (clojure.string/split line re))

(defn count-words [input-path output-path]
  (do-pipeline (from/text-file input-path) :debug
    (split-words #"\s+")
    (op/count)
    (to/text-file output-path)))

;====== average bytes by ip example ======
(fn-map parse-line [line] [:strings :clojure]
  (let [[address bytes] (clojure.string/split line #"\s+")]
    (pair-of address [(read-string bytes) 1])))

(fn-combine sum-bytes-and-counts [value1 value2]
  (mapv + value1 value2))

(fn-mapv compute-average [[bytes requests]] :ints
  (int (/ bytes requests)))

(defn count-bytes-by-ip [input-path output-path]
  (do-pipeline (from/text-file input-path)
    (parse-line)
    (op/group-by-key)
    (sum-bytes-and-counts)
    (compute-average)
    (to/text-file output-path)))

```

## License

Copyright © 2012 Victor Iacoban <victor.iacoban@gmail.com>

Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.
