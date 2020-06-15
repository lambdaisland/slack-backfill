(ns slack-backfill.json-lines
  (:require [cheshire.core :as json]
            [clojure.java.io :as io])
  (:import (java.io BufferedReader)))

(defn lines-reducible [^BufferedReader rdr]
  (reify clojure.lang.IReduceInit
    (reduce [this f init]
      (try
        (loop [state init]
          (if (reduced? state)
            state
            (if-let [line (.readLine rdr)]
              (recur (f state line))
              state)))
        (finally (.close rdr))))))

(def xform
  "Transducer which transforms a sequence of json-lines files into a sequence of values"
  (comp (mapcat #(lines-reducible (io/reader %)))
        (keep #(try
                 (json/parse-string %)
                 (catch Throwable e
                   (println "Error decoding JSON: " %)
                   (println e)
                   nil)))))

(defn append
  "Append a single form to a writer, followed by a newline."
  [writer value]
  (json/with-writer [writer]
    (json/write value))
  (.write writer "\n"))
