(defproject com.appsflyer/ketu "1.1.0-SNAPSHOT"
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

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/core.async "1.6.673"]
                 [expound "0.9.0"]
                 [org.apache.kafka/kafka-clients "3.3.1"]
                 [org.slf4j/slf4j-api "2.0.6"]]

  :profiles {;; REPL, development and testing
             :dev
             {:source-paths ["dev"]
              :plugins [[lein-cloverage "1.2.4"]
                        [lein-eftest "0.5.9"]]
              :dependencies [[org.clojure/tools.namespace "1.3.0"] ;For repl refresh
                             [tortue/spy "2.13.0"]
                             [metosin/sieppari "0.0.0-alpha13"]
                             [commons-io/commons-io "2.11.0"]
                             [ch.qos.logback/logback-classic "1.4.5"]
                             [org.clojure/test.check "1.1.1"]
                             ; Kafka (docker in docker)
                             [org.testcontainers/kafka "1.17.6"]
                             [clj-test-containers "0.7.4"]]
              :jvm-opts     ["-Dlogback.configurationFile=dev-logback.xml"]}

             ;; Tests only, silent logs
             :test
             {:jvm-opts ["-Dlogback.configurationFile=test-logback.xml"]}}

  :source-paths ["src"]
  :test-paths ["test"]
  :global-vars {*warn-on-reflection* true}
  :pedantic? :abort
  :javac-options ["-Xlint:deprecation"])
