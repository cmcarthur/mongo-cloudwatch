(ns cmcarthur.mongo-cloudwatch.core
  (:gen-class)
  (:require [monger.core :as mongo]
            [chime]
            [clj-time.periodic :as periodic]
            [clj-time.core :as time]
            [clojure.tools.cli :as cli]))

;; mongo

(def last-value (atom nil))

(defn get-mongo-stats
  "Given that a working mongo connection is already established, this function will
  pull stats from the mongo server and destructure them into something we care about."
  []
  (let [{host "host" {inserts "insert" queries "query" updates "update"} "opcounters" {{total "total"} "currentQueue"} "globalLock"} (mongo/command {:serverStatus 1})]
    {:host host :lock-queue-size total :query-totals {:inserts inserts :queries queries :updates updates}}))

(defn calculate-new-mongo-values
  "Given that a working mongo connection is already established, this function will
  pull some basic stats from the server: lock queue size, and number of queries run
  since the last run of this function.

  Since it takes a diff between calls, it's highly recommended that you call this
  function at a known interval."
  []
  (let [mongo-stats (get-mongo-stats)
        new-values (:query-totals mongo-stats)
        old-values (deref last-value)]
    (if (nil? old-values)
        (do (reset! last-value new-values) nil)
        {:host (:host mongo-stats)
         :lock-queue-size (:lock-queue-size mongo-stats)
         :query-totals (merge-with - new-values old-values)})))

;; cloudwatch

(defn create-push-metric-request
  "Wrapper for the PutMetricDataRequest class provided by the AWS Cloudwatch SDK."
  [namespace metric-data]
  (-> (com.amazonaws.services.cloudwatch.model.PutMetricDataRequest.)
      (.withNamespace namespace)
      (.withMetricData metric-data)))

(defn create-metric-datum
  "Wrapper for the MetricDatum class provided by the AWS Cloudwatch SDK."
  [dimensions name unit value]
  (-> (com.amazonaws.services.cloudwatch.model.MetricDatum.)
      (.withDimensions dimensions)
      (.withMetricName name)
      (.withUnit unit)
      (.withValue value)))

(defn create-dimension
  "Wrapper for the Dimension class provided by the AWS Cloudwatch SDK."
  [name value]
  (-> (com.amazonaws.services.cloudwatch.model.Dimension.)
      (.withName name)
      (.withValue value)))

(defn push-metric
  "Given a namespace and a metric data collection, this function will push
  the metric data to Cloudwatch."
  [namespace metric-data]
  (let [push-request (create-push-metric-request namespace metric-data)
        client (com.amazonaws.services.cloudwatch.AmazonCloudWatchClient.)]
    (.putMetricData client push-request)))

;; link

(defn create-cloudwatch-metrics-from-mongo-stats
  [mongo-stats]
  (let [{lock-queue-size :lock-queue-size {inserts :inserts queries :queries updates :updates} :query-totals} mongo-stats
        dimensions [(create-dimension "Hostname" (mongo-stats :host))]]
    [(create-metric-datum dimensions "Inserts" "Count" (double inserts))
     (create-metric-datum dimensions "Queries" "Count" (double queries))
     (create-metric-datum dimensions "Updates" "Count" (double updates))
     (create-metric-datum dimensions "Lock Queue Size" "Count" (double lock-queue-size))]))

(defn run-scheduler
  []
  (chime/chime-at (rest (periodic/periodic-seq (time/now)
                                               (time/secs 15)))
                  (fn [time]
                    (let [to-push (calculate-new-mongo-values)]
                      (when-not (nil? to-push)
                        (do (println "Pushing...")
                            (push-metric "Service/Mongo"
                                         (create-cloudwatch-metrics-from-mongo-stats to-push))))))))

(defn load-config
  [path]
  (with-open [^java.io.Reader reader (clojure.java.io/reader path)]
    (let [props (java.util.Properties.)]
      (.load props reader)
      (into {} (for [[k v] props] [(keyword k) (read-string v)])))))

(defn -main
  [& args]
  (let [[opts args banner]
        (cli/cli args
                 ["-c" "--config-file" "Specify an EDN config file to load"])]
    (let [config (load-config (-> opts :config-file))]
      (mongo/connect! (config :mongo))
      (mongo/use-db! "admin")
      (run-scheduler))))
