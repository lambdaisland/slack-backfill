(ns slack-backfill.datesplit
  (:gen-class)
  (:require [clojure.java.io :as io]
            [slack-backfill.json-lines :as json-lines])
  (:import (java.time Instant LocalDate ZoneId)))

(def UTC (ZoneId/of "UTC"))

(defn file-for-inst [files inst]
  (let [date (LocalDate/ofInstant inst UTC)
        fname (format "%d-%02d-%02d_backfill.txt" (.getYear date) (.getMonthValue date) (.getDayOfMonth date))]
    (if-let [file (get @files fname)]
      file
      (let [file (io/writer (io/file fname))]
        (swap! files assoc fname file)
        file))))

(defn slack-ts->inst [ts]
  (java.time.Instant/ofEpochMilli (Math/round (* (Double/parseDouble ts) 1000))))

(defn process [files]
  (let [fatom (atom {})]
    (doseq [{:strs [ts] :as msg} (sequence json-lines/xform files)
            :when ts]
      (let [f (file-for-inst fatom (slack-ts->inst ts))]
        (json-lines/append f msg)
        ))
    (doseq [f (vals @fatom)]
      (.close f))))

(defn -main [& files]
  (process files))
