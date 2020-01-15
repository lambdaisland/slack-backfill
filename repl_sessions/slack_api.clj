(require '[clj-slack.users]
         '[clj-slack.conversations]
         '[cheshire.core :as cheshire]
         '[clojure.java.io :as io])

(def slack-token (System/getenv "SLACK_TOKEN"))

(def connection {:api-url "https://slack.com/api" :token slack-token})

;; (with-open [file (io/writer "/tmp/test.txt")]
;;   (doseq [message (:messages result)]
;;     (cheshire/generate-stream message file)
;;     (.write file "\n")))

;; - for every channel
;; - fetch complete history
;; - reverse so its in chronological order
;; - save to file

;; - save channel information to a file
;; - save user information to a file

(defn fetch-channel-history [connection channel-id]
  (loop [batch   (clj-slack.conversations/history connection channel-id)
         history []]
    (let [history (apply conj history (:messages batch))]
      (if (:has_more batch)
        (let [cursor    (:next_cursor (:response_metadata batch))
              new-batch (clj-slack.conversations/history connection channel-id {:cursor cursor})]
          (recur new-batch history))
        history))))

(defn fetch-logs [connection]
  (let [conversations (clj-slack.conversations/list connection)
        channel-ids   (map #(:id %) (:channels conversations))]
    (for [channel-id channel-ids]
      (let [history (fetch-channel-history connection channel-id)]
        (with-open [file (io/writer (str "/tmp/channels/" channel-id ".txt"))]
          (doseq [message (reverse history)]
            (cheshire/generate-stream message file)
            (.write file "\n")))))))
