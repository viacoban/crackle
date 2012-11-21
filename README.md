## Crackle

A Clojure wrapper around [Apache Crunch](http://incubator.apache.org/crunch/)


## Installation

Crackle binaries not available yet, you'll have to build it yourself, with [gradle](http://gradle.org/)

## Usage

Word count example with Crackle:

```clj
(ns crackle.example
  (:use crackle.core))

(defn split-words [line]
  (clojure.string/split line #"\s+"))

(defn count-words [input-path output-path]
  (mem-pipeline (from-txt input-path) (to-txt output-path)
    (=each-to-seq split-words)
    (=count)))

```

## License

Copyright © 2012 Victor Iacoban <victor.iacoban@gmail.com>

Distributed under the Eclipse Public License, the same as Clojure.
