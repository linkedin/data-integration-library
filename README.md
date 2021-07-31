# Data Integration Library 
[![Build Status](https://github.com/linkedin/data-integration-library/actions/workflows/build-and-test.yml/badge.svg?branch=master)](https://github.com/linkedin/data-integration-library/actions?query=workflow%3A%22Build+and+Run+Tests%22+branch%3Amaster+event%3Apush)
[![Release Status](https://github.com/linkedin/data-integration-library/actions/workflows/release.yml/badge.svg?branch=master)](https://github.com/linkedin/data-integration-library/actions?query=workflow%3A%22Release+and+Publish%22+branch%3Amaster+event%3Apush)

LinkedIn Data Integration Library (DIL) is a collection of generic data integration components that can be mix-and-matched to form powerful ready-to-use connectors, which can then be used by data integration frameworks like [Apache Gobblin](https://gobblin.apache.org) or event processing frameworks like [Apache Kafka](https://kafka.apache.org/) to ingress or egress data between cloud services or APIs.    

# Highlights
- Generic components: data transmission protocol components and data format components are generically designed without one depending on another, greatly relieved the challenges in handling the variety of cloud APIs and services. 
- Multistage architecture: data integration is never a one-step process, the library inherently supports multi-staged integration processes so that complex data integration scenarios can be handled with simple generic components. 
- Bidirectional transmission: ingress and egress are just business logic in DIL, both work the same way and use the same set of configurations, as ingress to one end is egress to the other end.
- Extensible ompression and encryption: users can easily add pluggable and extensible data compression and encryption algorithms.
- Flexible pagaination: DIL supports a wide range of pagination methods to break large payloads to small chunks.

# Common Patterns used in production
- Asynchronous bulk ingestion from Rest APIs, like Salesforce.com, to Data Lake (HDFS, S3, ADLS)
- Data upload to Rest APIs, like Google API, with tracking of responses
- Ingest data from one Rest API and egress to another (Rest API) on cloud

# Requirements
* Java >= 1.8

If building the distribution with tests turned on:
* Maven version 3.5.3 

# Instructions to build the distribution
1. Extract the archive file to your local directory.
2. Set JAVA_HOME
3. Build
* Skip tests and build the distribution
> `./gradlew build -x findbugsMain -x test -x rat -x checkstyleMain` 

* Tests and build the distribution (requires Maven): 
> `./gradlew build` 
