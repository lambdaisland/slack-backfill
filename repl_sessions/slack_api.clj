(require '[clojure.string :as str]
         '[clj-slack.users]
         '[clj-slack.im]
         '[clj-slack.channels]
         '[cheshire.core :as cheshire]
         '[clojure.java.io :as io])

(def slack-token (System/getenv "SLACK_TOKEN"))

(def connection {:api-url "https://slack.com/api" :token slack-token})

(clj-slack.users/list connection)

(clj-slack.im/history connection "DN4E76Y9G")

(clj-slack.channels/list connection)

(def result (clj-slack.channels/history connection "CN8QF28C8"))

(:ts (last (:messages result)))

(def result (clj-slack.channels/history connection "CN8QF28C8" {:latest (:ts (last (:messages result)))
                                                                :count "1000"}))

(:messages result)

(with-open [file (io/writer "/tmp/test.txt")]
  (doseq [message (:messages result)]
    (cheshire/generate-stream message file)
    (.write file "\n")
    )
  )


;; - for every channel
;; - fetch complete history
;; - reverse so its in chronological order
;; - save to file

;; - save channel information to a file
;; - save user information to a file
