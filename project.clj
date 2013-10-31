(defproject mongo-cloudwatch "0.1.0-SNAPSHOT"
  :description "A clojure tool for pushing Mongo stats to Amazon Cloudwatch"
  :url "https://github.com/cmcarthur/mongo-cloudwatch"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.novemberain/monger "1.5.0"]]
  :main cmcarthur.mongo-cloudwatch.core)
