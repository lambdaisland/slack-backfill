(ns slack-backfill.core
  (:require [clj-slack.conversations]
            [clj-slack.users]
            [cheshire.core :as cheshire]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]))

(defn write-edn [filepath data]
  (with-open [writer (io/writer filepath)]
    (binding [*print-length* false
              *out*          writer]
      (pprint/pprint data))))

(defn user->tx [{:keys [id name is_admin is_owner profile]}]
  (let [{:keys [image_512 email real_name_normalized image_48 image_192
                real_name image_72 image_24 avatar_hash image_32 display_name
                display_name_normalized]} profile]
    (->> (merge #:user {:slack-id  id
                        :name      name
                        :real-name real_name
                        :admin?    is_admin
                        :owner?    is_owner}
                #:user-profile {:email                   email,
                                :avatar-hash             avatar_hash,
                                :image-32                image_32,
                                :image-24                image_24,
                                :image-192               image_192,
                                :image-48                image_48,
                                :real-name-normalized    real_name_normalized,
                                :display-name-normalized display_name_normalized,
                                :display-name            display_name,
                                :image-72                image_72,
                                :real-name               real_name,
                                :image-512               image_512})
         (remove (comp nil? val))
         (into {}))))

(defn fetch-users [target connection]
  (let [fetch-batch (partial clj-slack.users/list connection)
        filepath    (str target "/users.edn")]
    (.mkdirs (io/file target))
    (println "Fetching users data")
    (loop [batch (fetch-batch)
           users ()]
      (let [users (into users (:members batch))]
        (if (contains? batch :has_more)
          (let [cursor    (get-in batch [:response_metadata :next_cursor])
                new-batch (fetch-batch {:cursor cursor})]
            (recur new-batch users))
          (->> (map user->tx users)
               (write-edn filepath)))))))

(defn channel->tx [{:keys [id name created creator]}]
  #:channel {:slack-id id
             :name     name
             :created  created
             :creator  [:user/slack-id creator]})

(defn fetch-channels [target connection]
  (let [fetch-batch (partial clj-slack.conversations/list connection)
        filepath    (str target "/channels.edn")]
    (.mkdirs (io/file target))
    (println "Fetching channels data")
    (loop [batch    (fetch-batch)
           channels ()]
      (let [channels (into channels (:channels batch))]
        (if (contains? batch :has_more)
          (let [cursor    (get-in batch [:response_metadata :next_cursor])
                new-batch (fetch-batch {:cursor cursor})]
            (recur new-batch channels))
          (->> (map channel->tx channels)
               (write-edn filepath)))))))
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
