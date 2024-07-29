
# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [1.1.0] - 2024-29-07

### Added
- consumer decorator support.

## [1.0.0] - 2023-01-10

### Added
- NA
### Changed
- Use Kafka clients version 3.3.1.
  - **[Breaking changes](https://www.confluent.io/blog/apache-kafka-3-0-major-improvements-and-new-features/)** - Java clients library `org.apache.kafka/kafka-clients` upgraded from [2.5.1](https://kafka.apache.org/25/documentation.html) to [3.3.1](https://kafka.apache.org/33/documentation.html)
  - Java 11 or higher are supported
- Refresh dependencies versions:

| Dependency                        | From version | To version |
|-----------------------------------|--------------|------------|
| `org.clojure/clojure`             | 1.10.1       | 1.11.1     |
| `org.clojure/core.async`          | 1.3.610      | 1.6.673    |
| `expound`                         | 0.8.5        | 0.9.0      |
| `org.apache.kafka/kafka-clients`  | 2.5.1        | 3.3.1      |
| `org.slf4j/slf4j-api`             | 1.7.32       | 2.0.6      |

- Upgrade internal dependencies

| Dependency                       | From version | To version |
|----------------------------------|--------------|------------|
| `lein-cloverage`                 | 1.2.2        | 1.2.4      |
| `org.clojure/tools.namespace`    | 1.0.0        | 1.3.0      |
| `tortue/spy`                     | 2.0.0        | 2.13.0     |
| `commons-io/commons-io`          | 2.6          | 2.11.0     |
| `ch.qos.logback/logback-classic` | 1.2.3        | 1.4.5      |
| `org.clojure/test.check`         | 1.1.0        | 1.1.1      |
| `org.testcontainers/kafka`       | 1.16.2       | 1.17.6     |
| `clj-test-containers`            | 0.5.0        | 0.7.4      |

### Deprecated
- Deprecate checksum from Ketu source shape schema following deprecation notice from [ConsumerRecord](https://github.com/apache/kafka/pull/10470) version 3.0
- Java 8 support had been deprecated since Apache Kafka 3 - [here](https://kafka.apache.org/33/documentation.html#java)

### Fixed
- NA