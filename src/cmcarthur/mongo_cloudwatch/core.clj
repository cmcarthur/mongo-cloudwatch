(ns cmcarthur.mongo-cloudwatch.core
  (:require [monger.core :as mongo]))

(def last-value (atom nil))

(defn get-mongo-stats
  []
  (let [{{inserts "insert" queries "query" updates "update"} "opcounters" {{total "total"} "currentQueue"} "globalLock"} (mongo/command {:serverStatus 1})]
    {:lock-queue-size total :query-totals {:inserts inserts :queries queries :updates updates}}))

(defn calculate-new-mongo-values
  []
  (let [mongo-stats (get-mongo-stats)
        new-values (:query-totals mongo-stats)
        old-values (deref last-value)]
    (if (nil? old-values)
        (do (reset! last-value new-values) nil)
        {:lock-queue-size (:lock-queue-size mongo-stats) :query-totals (merge-with - new-values old-values)})))

(defn create-push-metric-request
  [namespace metric-data]
  (-> (com.amazonaws.services.cloudwatch.model.PutMetricDataRequest.)
      (.withNamespace namespace)
      (.withMetricData metric-data)))

(defn create-metric-datum
  [dimensions name unit value]
  (-> (com.amazonaws.services.cloudwatch.model.MetricDatum.)
      (.withDimensions dimensions)
      (.withMetricName name)
      (.withUnit unit)
      (.withValue value)))

(defn create-dimension
  [name value]
  (-> (com.amazonaws.services.cloudwatch.model.Dimension.)
      (.withName name)
      (.withValue value)))

(defn push-metric
  [namespace metric-data]
  (let [push-request (create-push-metric-request namespace metric-data)
        client (com.amazonaws.services.cloudwatch.AmazonCloudWatchClient.)]
    (.putMetricData client push-request)))
