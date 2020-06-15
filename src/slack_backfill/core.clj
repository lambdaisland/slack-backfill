(ns slack-backfill.core
  (:require [clj-slack.conversations :as slack-conv]
            [clj-slack.users :as slack-users]
            [cheshire.core :as cheshire]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]))

(defn wrap-rate-limit [f]
  (fn invoke [& args]
    (try
      (apply f args)
      (catch clojure.lang.ExceptionInfo ex
        (let [data (ex-data ex)]
          (if (= 429 (:status data))
            (let [wait-for (Integer/parseInt (get-in data [:headers "retry-after"]))]
              (println "rate-limited. Retry in" wait-for "seconds")
              (Thread/sleep (* (inc wait-for) 1000))
              (apply invoke args))
            (throw ex)))))))

(def slack-users-list (wrap-rate-limit slack-users/list))
(def slack-conv-list (wrap-rate-limit slack-conv/list))
(def slack-conv-history (wrap-rate-limit slack-conv/history))

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
  (let [fetch-batch (partial slack-users-list connection)
        filepath    (str target "/users.edn")]
    (.mkdirs (io/file target))
    (println "Fetching users data")
    (loop [batch (fetch-batch)
           users ()]
      (let [users (into users (:members batch))
            cursor (get-in batch [:response_metadata :next_cursor])]
        (if (empty? cursor)
          (->> (map user->tx users)
               (write-edn filepath))
          (let [new-batch (fetch-batch {:cursor cursor})]
            (recur new-batch users)))))))

(defn channel->tx [{:keys [id name created creator]}]
  #:channel {:slack-id id
             :name     name
             :created  created
             :creator  [:user/slack-id creator]})

(defn fetch-channels [target connection]
  (let [fetch-batch (partial slack-conv-list connection)
        filepath    (str target "/channels.edn")]
    (.mkdirs (io/file target))
    (println "Fetching channels data")
    (loop [batch    (fetch-batch)
           channels ()]
      (let [channels (into channels (:channels batch))
            cursor (get-in batch [:response_metadata :next_cursor])]
        (if (empty? cursor)
          (->> (map channel->tx channels)
               (write-edn filepath))
          (let [new-batch (fetch-batch {:cursor cursor})]
            (recur new-batch channels)))))))

(defn fetch-channel-history [connection channel-id]
  (let [fetch-batch (partial slack-conv-history connection channel-id)]
    (loop [batch   (fetch-batch)
           history ()]
      (let [history (into history (:messages batch))
            cursor  (get-in batch [:response_metadata :next_cursor])]
        (if (empty? cursor)
          (map #(assoc % :channel channel-id) history)
          (let [new-batch (fetch-batch {:cursor cursor})]
            (recur new-batch history)))))))

(defn save-logs [conversations target connection]
  (let [channel-ids   (:channels conversations)]
    (doseq [{channel-id :id
             channel-name :name} channel-ids]
      (println "Fetching" channel-id)
      (let [history (fetch-channel-history connection channel-id)]
        (with-open [file (io/writer (str target "/000_" channel-id "_" channel-name ".txt"))]
          (doseq [message history]
            (cheshire/generate-stream message file)
            (.write file "\n")))))))

(defn fetch-logs [target connection]
  (let [logs-target (str target "/logs")]
    (.mkdirs (io/file logs-target))
    (loop [conversations (slack-conv-list connection)]
      (save-logs conversations logs-target connection)
      (let [cursor (get-in conversations [:response_metadata :next_cursor])]
        (when (seq cursor)
          (recur (slack-conv-list connection {:cursor cursor})))))))

(defn conn
  ([]
   (conn (System/getenv "SLACK_TOKEN")))
  ([slack-token]
   {:api-url "https://slack.com/api" :token slack-token}))

(defn -main
  "Takes a path like /tmp/test (without a trailing slash), fetches users and
  channels data into path root and channel logs into a /logs subdirectory."
  [target]
  (let [connection (conn)]
    (fetch-channels target connection)
    (fetch-users target connection)
    (fetch-logs target connection)))

(comment
  (fetch-logs "/tmp/backfill" (conn))
  (-main "/tmp/test"))
