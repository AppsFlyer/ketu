# Apache Kafka Client Library Changes Analysis

## Version 3.3.1 to 3.9.1

**Analysis Date:** September 2025  
**Library:** org.apache.kafka/kafka-clients  
**Version Range:** 3.3.1 → 3.9.1

---

## Executive Summary

This document provides a comprehensive analysis of all changes between Apache Kafka client library versions 3.3.1 and
3.9.1, with particular focus on breaking changes, configuration modifications, and default behavior changes. The
analysis is organized with breaking changes listed first, followed by configuration changes and security updates.

**Important Note:** Not all assumed intermediate versions (3.4.0, 3.5.0, 3.6.0, etc.) may have been released as major
versions. The analysis focuses on documented changes across the version range.

---

## BREAKING CHANGES

| Version   | Change Type                  | Configuration/Component                                                       | Description                                                                                                           | Impact                                                                          | Source                                                                                                  |
|-----------|------------------------------|-------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| **3.9.1** | **Security Breaking Change** | `sasl.jaas.config`                                                            | Disabled `com.sun.security.auth.module.JndiLoginModule` and `com.sun.security.auth.module.LdapLoginModule` by default | Users must explicitly allow these modules if needed                             | [Apache Kafka CVE List](https://kafka.apache.org/cve-list.html)                                         |
| **3.9.1** | **Security Breaking Change** | `sasl.oauthbearer.token.endpoint.url`<br>`sasl.oauthbearer.jwks.endpoint.url` | Introduced `org.apache.kafka.sasl.oauthbearer.allowed.urls` system property                                           | In 3.9.1, all URLs accepted by default; in 4.0.0+, empty list by default        | [Apache Kafka CVE List](https://kafka.apache.org/cve-list.html)                                         |
| **3.9.0** | **Deprecation**              | `delete-config` option in `kafka-topics.sh`                                   | Deprecated `delete-config` option                                                                                     | Users should use `--alter --delete-config` with `kafka-configs.sh` or Admin API | [Confluent Platform Release Notes](https://docs.confluent.io/platform/current/release-notes/index.html) |
| **3.9.0** | **Deprecation**              | `offsets.commit.required.acks`                                                | Deprecated configuration                                                                                              | Will be removed in Kafka 4.0                                                    | [Apache Kafka Blog](https://kafka.apache.org/blog)                                                      |
| **3.9.0** | **Deprecation**              | Log4J Appender                                                                | Deprecated Log4J Appender                                                                                             | Expected to be removed in Kafka 4.0                                             | [Apache Kafka Blog](https://kafka.apache.org/blog)                                                      |
| **3.9.0** | **Deprecation**              | `kafka.serializer.Decoder`                                                    | Deprecated `kafka.serializer.Decoder`                                                                                 | Replaced by `org.apache.kafka.tools.api.Decoder`                                | [Apache Kafka Blog](https://kafka.apache.org/blog)                                                      |
| **3.8.0** | **Deprecation**              | `offsets.commit.required.acks`                                                | Deprecated configuration                                                                                              | Will be removed in Kafka 4.0                                                    | [Apache Kafka Blog](https://kafka.apache.org/blog)                                                      |
| **3.8.0** | **Deprecation**              | Log4J Appender                                                                | Deprecated Log4J Appender                                                                                             | Expected to be removed in Kafka 4.0                                             | [Apache Kafka Blog](https://kafka.apache.org/blog)                                                      |
| **3.8.0** | **Deprecation**              | `kafka.serializer.Decoder`                                                    | Deprecated `kafka.serializer.Decoder`                                                                                 | Replaced by `org.apache.kafka.tools.api.Decoder`                                | [Apache Kafka Blog](https://kafka.apache.org/blog)                                                      |
| **3.7.0** | **Deprecation**              | Client APIs prior to 2.1                                                      | Client APIs released before Kafka 2.1 marked as deprecated                                                            | Will be removed in Kafka 4.0                                                    | [Apache Kafka 3.7.0 Release Announcement](https://kafka.apache.org/blog)                                |
| **3.7.0** | **Deprecation**              | Java 11 Support                                                               | Java 11 support for Kafka broker deprecated                                                                           | Planned for removal in Kafka 4.0                                                | [Apache Kafka 3.7.0 Release Announcement](https://kafka.apache.org/blog)                                |
| **3.5.0** | **Deprecation**              | ZooKeeper                                                                     | ZooKeeper marked as deprecated                                                                                        | Planned for removal in Kafka 4.0                                                | [Apache Kafka Blog](https://kafka.apache.org/blog)                                                      |

---

## CONFIGURATION CHANGES AND DEFAULT BEHAVIOR MODIFICATIONS

| Version   | Configuration Parameter                                                       | Change Type              | Description                                                          | Impact                                                          | Source                                                                                                                         |
|-----------|-------------------------------------------------------------------------------|--------------------------|----------------------------------------------------------------------|-----------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| **3.9.1** | `org.apache.kafka.disallowed.login.modules`                                   | **New System Property**  | Added system property to disable specific login modules in SASL JAAS | Enhances security by preventing use of vulnerable modules       | [Apache Kafka CVE List](https://kafka.apache.org/cve-list.html)                                                                |
| **3.9.1** | `sasl.jaas.config`                                                            | **Security Fix**         | Fixed deserialization of untrusted data vulnerability                | Users should upgrade to 3.9.1+ and review configurations        | [Snyk Vulnerability Report](https://security.snyk.io/package/maven/org.apache.kafka%3Akafka-clients/3.9.0)                     |
| **3.9.1** | `sasl.oauthbearer.token.endpoint.url`<br>`sasl.oauthbearer.jwks.endpoint.url` | **Security Fix**         | Fixed Server-Side Request Forgery (SSRF) vulnerability               | Users should validate and restrict these configurations         | [Snyk Vulnerability Report](https://security.snyk.io/package/maven/org.apache.kafka%3Akafka-clients/3.9.0)                     |
| **3.9.1** | Java 23 Support                                                               | **New Feature**          | Added support for Java 23                                            | Applications must ensure compatibility with Java 23             | [Apache Kafka 3.9.1 Release Announcement](https://kafkacommunity.blogspot.com/2025/05/announce-apache-kafka-391.html)          |
| **3.8.0** | `remote.fetch.max.wait.ms`                                                    | **New Configuration**    | New timeout parameter for delayed remote fetch requests              | Allows users to configure timeout based on workload             | [Apache Kafka Blog](https://kafka.apache.org/blog)                                                                             |
| **3.7.0** | `org.apache.kafka.automatic.config.providers`                                 | **New System Property**  | System property to disable automatic config providers                | Setting to `none` can mitigate security vulnerabilities         | [IBM Security Bulletin](https://www.ibm.com/support/pages/security-bulletin-kafka-client-library-upgraded-kafka-clients-391-1) |
| **3.7.0** | `ConfigProviders` interface                                                   | **Security Fix**         | Addressed vulnerability allowing reading arbitrary disk contents     | Users should upgrade and configure appropriate allowlists       | [Snyk Vulnerability Report](https://security.snyk.io/package/maven/org.apache.kafka%3Akafka-clients/3.1.1)                     |
| **3.3.0** | `replica.lag.max.messages`                                                    | **Removed**              | Parameter removed                                                    | Partition leaders no longer consider lagging message count      | [Confluent Platform 3.3.0 Release Notes](https://docs.confluent.io/legacy/platform/3.3.0/release-notes.html)                   |
| **3.3.0** | `replica.lag.time.max.ms`                                                     | **Behavior Change**      | Now refers to time since replica last caught up                      | Replicas not caught up within time are considered out of sync   | [Confluent Platform 3.3.0 Release Notes](https://docs.confluent.io/legacy/platform/3.3.0/release-notes.html)                   |
| **3.3.0** | `reserved.broker.max.id`                                                      | **Default Value Change** | Broker IDs above 1000 now reserved by default                        | If existing broker IDs exceed threshold, increase configuration | [Confluent Platform 3.3.0 Release Notes](https://docs.confluent.io/legacy/platform/3.3.0/release-notes.html)                   |

---

## SECURITY VULNERABILITIES ADDRESSED

| Version   | CVE/Security Issue            | Configuration Affected                                                        | Description                                    | Source                                                                                                     |
|-----------|-------------------------------|-------------------------------------------------------------------------------|------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| **3.9.1** | Deserialization Vulnerability | `sasl.jaas.config`                                                            | Fixed improper handling of configuration data  | [Snyk Vulnerability Report](https://security.snyk.io/package/maven/org.apache.kafka%3Akafka-clients/3.9.0) |
| **3.9.1** | SSRF Vulnerability            | `sasl.oauthbearer.token.endpoint.url`<br>`sasl.oauthbearer.jwks.endpoint.url` | Fixed improper handling of URL configurations  | [Snyk Vulnerability Report](https://security.snyk.io/package/maven/org.apache.kafka%3Akafka-clients/3.9.0) |
| **3.7.0** | ConfigProvider Vulnerability  | `ConfigProviders` interface                                                   | Fixed unauthorized access to files/directories | [Snyk Vulnerability Report](https://security.snyk.io/package/maven/org.apache.kafka%3Akafka-clients/3.1.1) |

---

## DETAILED BREAKING CHANGES ANALYSIS

### 1. SASL JAAS Configuration Changes (3.9.1)

**Impact:** HIGH - Security-related breaking change

- **What Changed:** Default behavior now disables `JndiLoginModule` and `LdapLoginModule`
- **Why:** Security vulnerability mitigation
- **Action Required:** If using these modules, explicitly configure them or use alternative authentication methods

### 2. OAuth Bearer URL Restrictions (3.9.1)

**Impact:** MEDIUM - Configuration change with future breaking change

- **What Changed:** New system property `org.apache.kafka.sasl.oauthbearer.allowed.urls`
- **Why:** Prevent SSRF attacks
- **Action Required:** In Kafka 4.0+, URLs must be explicitly allowed (empty list by default)

### 3. ZooKeeper Deprecation (3.5.0)

**Impact:** HIGH - Major architectural change

- **What Changed:** ZooKeeper marked as deprecated
- **Why:** Migration to KRaft mode for better performance and simplicity
- **Action Required:** Plan migration to KRaft mode before Kafka 4.0

### 4. Java 11 Deprecation (3.7.0)

**Impact:** MEDIUM - Runtime environment change

- **What Changed:** Java 11 support deprecated for Kafka broker
- **Why:** Focus on newer Java versions
- **Action Required:** Upgrade to Java 17+ for future compatibility

---

## MIGRATION RECOMMENDATIONS

### Immediate Actions (Version 3.9.1)

1. **Upgrade to 3.9.1** for critical security fixes
2. **Review SASL configurations** for disabled login modules
3. **Validate OAuth Bearer URLs** and prepare for future restrictions
4. **Test Java 23 compatibility** if planning to use Java 23

### Medium-term Planning (Before Kafka 4.0)

1. **Migrate from ZooKeeper to KRaft** mode
2. **Update deprecated client APIs** (pre-2.1 APIs)
3. **Replace deprecated configurations:**
    - `offsets.commit.required.acks`
    - Log4J Appender
    - `kafka.serializer.Decoder`
4. **Plan Java version upgrade** (away from Java 11)

### Configuration Updates Required

```properties
# New security-related system properties (3.9.1)
-Dorg.apache.kafka.disallowed.login.modules=com.sun.security.auth.module.JndiLoginModule,com.sun.security.auth.module.LdapLoginModule
-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://trusted-oauth-provider.com
# Disable automatic config providers (3.7.0+)
-Dorg.apache.kafka.automatic.config.providers=none
# New configuration parameter (3.8.0)
remote.fetch.max.wait.ms=5000
```

---

## RISK ASSESSMENT

### High Risk Changes

- **SASL JAAS module restrictions** - May break existing authentication
- **ZooKeeper deprecation** - Requires architectural changes
- **Security vulnerabilities** - Must upgrade to 3.9.1

### Medium Risk Changes

- **Java version requirements** - Runtime environment changes
- **Deprecated configurations** - Future breaking changes
- **OAuth Bearer URL restrictions** - Future configuration requirements

### Low Risk Changes

- **New configuration parameters** - Optional additions
- **New system properties** - Optional security enhancements

---

## TESTING RECOMMENDATIONS

1. **Security Testing**
    - Test SASL authentication with new module restrictions
    - Validate OAuth Bearer URL configurations
    - Verify ConfigProvider security fixes

2. **Compatibility Testing**
    - Test with Java 17+ and Java 23
    - Verify deprecated API replacements
    - Test new configuration parameters

3. **Performance Testing**
    - Benchmark with new `remote.fetch.max.wait.ms` setting
    - Test KRaft mode performance vs ZooKeeper
    - Validate security overhead impact

---

## CONCLUSION

The upgrade from Kafka client 3.3.1 to 3.9.1 includes significant security improvements, architectural changes, and
deprecations that will become breaking changes in Kafka 4.0. The most critical changes are:

1. **Security fixes in 3.9.1** - Should be prioritized for immediate upgrade
2. **ZooKeeper deprecation** - Requires planning for KRaft migration
3. **Java version requirements** - Runtime environment updates needed
4. **Configuration changes** - Various deprecated parameters need attention

**Recommendation:** Plan a phased upgrade approach, starting with 3.9.1 for security fixes, followed by architectural
changes (ZooKeeper → KRaft) and configuration updates before Kafka 4.0 release.

---

*This analysis is based on publicly available release notes, security advisories, and documentation. For the most
current information, always refer to the official Apache Kafka release notes and upgrade guides.*
