(ns slack-backfill.core
  (:require [clj-slack.conversations]
            [clj-slack.users]
            [cheshire.core :as cheshire]
            [clojure.java.io :as io]))

(defn write-edn [filepath data]
  (with-open [writer (io/writer filepath)]
    (binding [*print-length* false
              *out* writer]
      (pr data))))

(defn fetch-channels [target connection]
  (.mkdirs (io/file target))
  (println "Fetching channels data")
  (let [channels (clj-slack.conversations/list connection)]
    (write-edn (str target "/channels.edn") channels)))

(defn fetch-users [target connection]
  (.mkdirs (io/file target))
  (println "Fetching users data")
  (let [users (clj-slack.users/list connection)]
    (write-edn (str target "/users.edn") users)))

(defn fetch-channel-history [connection channel-id]
  (loop [batch   (clj-slack.conversations/history connection channel-id)
         history ()]
    (let [history (into history (:messages batch))]
      (if (:has_more batch)
        (let [cursor    (:next_cursor (:response_metadata batch))
              new-batch (clj-slack.conversations/history connection channel-id {:cursor cursor})]
          (recur new-batch history))
        history))))

(defn fetch-logs [target connection]
  (let [logs-target (str target "/logs")]
    (.mkdirs (io/file logs-target))
    (let [conversations (clj-slack.conversations/list connection)
          channel-ids   (map :id (:channels conversations))]
      (doseq [channel-id channel-ids]
        (println "Fetching" channel-id)
        (let [history (fetch-channel-history connection channel-id)]
          (with-open [file (io/writer (str logs-target "/" channel-id ".txt"))]
            (doseq [message history]
              (cheshire/generate-stream message file)
              (.write file "\n"))))))))

(defn -main
  "Takes a path like /tmp/test (without a trailing slash), fetches users and
  channels data into path root and channel logs into a /logs subdirectory."
  [target]
  (let [slack-token (System/getenv "SLACK_TOKEN")
        connection  {:api-url "https://slack.com/api" :token slack-token}]
    (fetch-channels target connection)
    (fetch-users target connection)
    (fetch-logs target connection)))

(comment
  (-main "/tmp/test"))
