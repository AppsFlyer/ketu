(defproject com.appsflyer/ketu "0.6.1-SNAPSHOT"
  :description "Clojure Apache Kafka client with core.async api"
  :url "https://github.com/AppsFlyer/ketu"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}

  :deploy-repositories [["releases" {:url "https://repo.clojars.org"
                                     :sign-releases false
                                     :username :env/clojars_user
                                     :password :env/clojars_pass}]
                        ["snapshots" {:url "https://repo.clojars.org"
                                      :username :env/clojars_user
                                      :password :env/clojars_pass}]]

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.3.610"]
                 [expound "0.8.5"]
                 [org.apache.kafka/kafka-clients "2.5.1"]
                 [org.slf4j/slf4j-api "1.7.30"]]

  :profiles {;; REPL, development and testing
             :dev
             {:source-paths ["dev"]
              :plugins [[lein-cloverage "1.2.2"]]
              :dependencies [[org.clojure/tools.namespace "1.0.0"] ;For repl refresh
                             [tortue/spy "2.0.0"]
                             [metosin/sieppari "0.0.0-alpha13"]
                             [commons-io/commons-io "2.6"]
                             [ch.qos.logback/logback-classic "1.2.3"]
                             [org.apache.kafka/kafka_2.12 "2.5.1"]]
              :jvm-opts ["-Dlogback.configurationFile=dev-logback.xml"]}

             ;; Tests only, silent logs
             :test
             {:jvm-opts ["-Dlogback.configurationFile=test-logback.xml"]}}

  :source-paths ["src"]
  :test-paths ["test"]
  :global-vars {*warn-on-reflection* true}
  :pedantic? :abort
  :javac-options ["-Xlint:deprecation"])
