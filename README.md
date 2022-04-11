# Data Integration Library 
[![Build Status](https://github.com/linkedin/data-integration-library/actions/workflows/build-and-test.yml/badge.svg?branch=master)](https://github.com/linkedin/data-integration-library/actions?query=workflow%3A%22Build+and+Run+Tests%22+branch%3Amaster+event%3Apush)
[![Release Status](https://github.com/linkedin/data-integration-library/actions/workflows/release.yml/badge.svg?branch=master)](https://github.com/linkedin/data-integration-library/actions?query=workflow%3A%22Release+and+Publish%22+branch%3Amaster+event%3Apush)

LinkedIn Data Integration Library (DIL) is a collection of generic data integration components that can be mix-and-matched to form powerful ready-to-use connectors, which can then be used by data integration frameworks like [Apache Gobblin](https://gobblin.apache.org) or event processing frameworks like [Apache Kafka](https://kafka.apache.org/) to ingress or egress data between cloud services or APIs.    

# Highlights
- Generic components: data transmission protocol components and data format components are generically designed without one depending on another, greatly relieved the challenges in handling the variety of cloud APIs and services. 
- Multistage architecture: data integration is never a one-step process, the library inherently supports multi-staged integration processes so that complex data integration scenarios can be handled with simple generic components. 
- Bidirectional transmission: ingress and egress are just business logic in DIL, both work the same way and use the same set of configurations, as ingress to one end is egress to the other end.
- Extensible compression and encryption: users can easily add pluggable and extensible data compression and encryption algorithms.
- Flexible pagination: DIL supports a wide range of pagination methods to break large payloads to small chunks.

# Common Patterns used in production
- Asynchronous bulk ingestion from Rest APIs, like Salesforce.com, to Data Lake (HDFS, S3, ADLS)
- Data upload to Rest APIs, like Google API, with tracking of responses
- Ingest data from one Rest API and egress to another (Rest API) on cloud

# Requirements
* JDK 1.8

If building the distribution with tests turned on:
* Maven version 3.5.3 

# Instructions to build the distribution
1. Extract the archive file to your local directory.
2. Set JAVA_HOME to use JDK 1.8 (JDK 11+ not supported)
3. Build
> `./gradlew build` 

# Instructions to contribute 
To contribute, please use submit Pull Request (PR) for committers to merge. 
1. Create your own fork on GitHub off the main repository
2. Clone your fork to your local computer
    - `git clone https://github.com/<<your-github-login>>/data-integration-library.git`
3. Add upstream and verify
    - `git remote add upstream https://github.com/linkedin/data-integration-library.git`
    - `git remote -v`
4. Change, test, commit, and push to your fork
    - `git status`
    - `git add .`
    - `git commit -m "comments"`
    - `git push origin master`
5. Create Pull Request on GitHub with the following details
    - Title 
    - Detailed description
    - Document the tests done
    - Links to the updated documents
6. Publish to local Maven repository
    - `./gradlew publishToMavenLocal`
7. Refresh your fork
    - if upstream has no conflict with your fork, you can go to your forked
    repository, and use "Fetch upstream" function to sync up your fork.
    - if upstream has conflicts with your fork, GitHub will ask you to create
    a pull request to merge. 
        - if the conflicts are too significant, it is better to just copy
        everything from upstream (the main repository) to your fork; that can
        be done with the following procedure:
            - Follow step 2 and step 3 above
            - `git fetch upstream`
            - `git reset --hard upstream/master`
            - `git push origin +master`
            - check your fork should be in sync with the main repository

# User Guides

- [1. Job Properties](docs/parameters/summary.md)
- [2. Job Properties by Category](docs/parameters/categories.md)
- [3. Deprecated Job Properties](docs/parameters/deprecated.md)
- [4. Flow Design Patterns](docs/patterns/summary.md)
- [5. How-tos](docs/how-to/summary.md)
- [6. Key Concepts](docs/concepts/summary.md)
- [7. Key Components](docs/components/summary.md)
- [8. Sample Flow Configurations](docs/sample-configs/summary.md)