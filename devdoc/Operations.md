# Operational Documentation

This document serves as a comprehensive guide for system administrators, providing insights and instructions from developers to ensure smooth operations.

## General HPCC Platform Documents

[Version Support](VersionSupport.md)
Details on Supported Versions

[Red Book](https://hpccsystems.atlassian.net/wiki/spaces/hpcc/pages/23586808/HPCC+Systems+Red+Book)
The Platform Red Book provides notices about changes in platform releases that may require some changes to coding practices or operational procedures.

[Platform Overview](../README.md)
Overview of the HPCC Systems Platform.

[File Processing](NewFileProcessing.md)
Guide for implementing new file processing features.

[Memory Manager](MemoryManager.md)
Documentation on memory management strategies.

[Data Masking](../system/masking/include/readme.md)
Documentation for the data obfuscation framework.

[DataMasker plugin](../system/masking/plugins/datamasker/readme.md)
Documentation for DataMasker plugin.

[ESP API tool](../tools/esp-api/README.md)
Documentation for ESP API tool.

## Azure

Details about using the HPCC Systems platform in Azure.

[Azure Tips And Tricks](./userdoc/azure/TipsAndTricks.md)
General Azure deployment tips for HPCC Systems Platform.

## Containerized Deployment

### Docker

Details about using Docker and Docker Desktop.

[Docker Images](../dockerfiles/README.md)
General information about Docker Images in the repository.

### Helm

The HPCC Systems Platform uses Helm (a package manager for Kubernetes) to deploy and manage clusters by providing Helm charts that encapsulate all Kubernetes resources needed to run HPCC Systems.

[Helm Examples](./helmReadmes.md) General documentation for the Helm examples.

## Dali

Dali is the central metadata repository and coordination service in the HPCC Systems Platform. It plays a critical role in managing and storing system metadata, including file information, cluster configurations, and workunit details. Dali ensures consistency and synchronization across the platform, enabling efficient resource management and job execution.

### DaliAdmin

The `DaliAdmin` tool is a command-line utility designed for managing and troubleshooting the Dali server. It provides various commands to interact with the Dali server, enabling system administrators to perform critical operations efficiently.

## Sasha

Guidelines for handling the Sasha server, including maintenance and configuration.
[Sasha Readme](/dali/sasha/sasha.md)

## DFU

Details on the Distributed File Utility (DFU), its usage, and best practices.

### DFU XREF

The DFU XREF (Cross-Reference) tool is used to identify and resolve inconsistencies in the logical file metadata stored in the HPCC Systems environment. It ensures that the metadata accurately reflects the physical files present in the system.

## DFUPlus

DFUPlus is a command-line utility that provides advanced file management capabilities within the HPCC Systems Platform. It is designed to interact with the Distributed File Utility (DFU) service, enabling users to perform file operations programmatically or through scripts.

[Client Tools Documentation](https://hpccsystems.com/training/documentation/ecl-ide-and-client-tools/)

## ECL

### ECL IDE

The ECL IDE is the simple and easy way to create Queries into your data, and ECL files with which to build your queries.

[Client Tools Documentation](https://hpccsystems.com/training/documentation/ecl-ide-and-client-tools/)

### ECL Language Extension for Visual Studio Code

This extension adds rich language support for HPCC Systems ECL language for the HPCC-Platform to VS Code.

[ECL Extension on Visual Studio Marketplace](https://marketplace.visualstudio.com/items?itemName=hpcc-systems.ecl)

## ECL Watch

Instructions for monitoring and managing the system using ECL Watch.

## ECLCC

Information on the ECL Compiler (ECL CC) and its operational aspects.

## ECL Agent / hThor

Operational details for ECL Agent and hThor, including job execution and monitoring.

## EVTool

Documentation on using EVTool for system diagnostics and analysis.

## ESDL

Comprehensive instructions for working with Enterprise Services Definition Language (ESDL) and the ESDL Tools.

[ESDL ReadMe](../esp/esdllib/README.md)
General documentation for ESDL library.

[ESDL Command Line Tool](../tools/esdlcmd/README.md)
Documentation for ESDL command-line tool.

[ESP API tool](../tools/esp-api/README.md)
Documentation for ESP API tool.

[ESDL Security Details](../esp/esdllib/README-SECURITY.md)  
Security-related documentation for ESDL library.

[ESDL functions](ESDLFunctions.md) List of ESDL Functions with links to details.

## Security

[Security User Authentication](SecurityUserAuthentication.md)
User authentication mechanisms and configuration.

[Security Configuration](SecurityConfig.md)
Configuration options for HPCC Systems Platform security.

[JWT Security plugin](../system/security/plugins/jwtSecurity/README.md)
Documentation for JWT Security plugin.

### LDAP

[LDAP Security](LDAPSecurityManager.md)
Steps for configuring and managing LDAP for authentication and authorization.

[Creating an LDAP DS for testing](DevTestWithLDAP.md)
Step-by-step instructions for configuring and testing HPCC Systems platform authentication and authorization using a containerized LDAP Directory Server, both on bare-metal and Kubernetes deployments.

### Secrets

Best practices for managing secrets securely within the system.

- [Kubernetes Secrets](../helm/examples/secrets/README-kubernetessecrets.md)

- [Vault Secrets K8s Authentication](../helm/examples/secrets/README-vault_secrets_using_kubernetes_authentication.md)

- [Vault Secrets Client Cert Vault Authentication](../helm/examples/secrets/README-vaultsecretsusingclientcertvaultauthentication.md)

## Roxie

Roxie is the query processing engine, optimized for real-time data delivery. It is designed to handle high-concurrency, low-latency queries efficiently, making it ideal for queries requiring rapid responses.

[Roxie Technical Details](roxie.md)
Technical details and usage of the Roxie engine.

[Roxie FAQ](./userdoc/roxie/FAQ.md)  
Frequently asked questions about Roxie.

[Optimizing Roxie Query Performance](OptimizingRoxieQueryPerformance.md)
Explains how to configure Roxie to maintain consistent query response times by setting minimum execution thresholds through various methods including configuration files, ECL code options, and URL parameters. It also describes how to monitor performance.

### [OptimizingRoxieQueryPerformance](OptimizingRoxieQueryPerformance.md)

Explains how to configure Roxie to maintain consistent query response times by setting minimum execution thresholds through various methods including configuration files, ECL code options, and URL parameters. It also describes how to monitor performance.

## Workunits

In HPCC Systems, workunits are central to the execution of ECL (Enterprise Control Language) queries. Each query submitted to the system generates a unique workunit ID (WUID), which can be used to track and manage the job. It represents the lifecycle of a job from submission to completion, including all the metadata, inputs, outputs, and logs associated with it.

For an explanation of workunits, and a walk-through of the steps in executing a query, see:
[Workunit Workflow](Workunits.md)

## Metrics Framework

The Metrics Framework in HPCC Systems is a system designed to collect, manage, and report metrics across various components of the platform. Metrics are quantitative measurements that provide insights into the performance, resource usage, and operational health of the system.

For more details, you can refer to the linked Metrics Framework Design in your documentation. [Metrics Framework Design](Metrics.md)

## Logging

Logging in the HPCC Systems Platform provides a robust mechanism for tracking system activities, diagnosing issues, and monitoring performance. Logs are generated by various components and services, offering detailed insights into operations, errors, and events. Developers can use these logs to troubleshoot problems, optimize performance, and ensure system reliability.

## Regression Suite

The Regression Suite in the HPCC Systems Platform is a comprehensive testing framework designed to ensure the stability, reliability, and correctness of the system. It consists of a collection of automated tests that validate the functionality of various components and features across different platform versions.

### Regression Suite Key Features

- **Automated Testing**: Execute a wide range of tests automatically to verify system behavior.
- **Version Compatibility**: Ensure that new changes do not break compatibility with previous versions.
- **Extensibility**: Add custom tests to cover specific use cases or new features.
- **Detailed Reporting**: Generate reports to analyze test results and identify issues.

## Plugins

Plugins in the HPCC Systems Platform are used to extend the platform's capabilities, typically by integrating external libraries, custom algorithms, or supporting new data formats and protocols.

[Couchbase](../plugins/couchbase/README.md)
Documentation for the Couchbase plugin.

[DataMasker plugin](../system/masking/plugins/datamasker/readme.md)
Documentation for DataMasker plugin.

[ECL BLAS](../plugins/eclblas/README.md)
Documentation for the ECL BLAS plugin.

[Example plugin](../plugins/exampleplugin/README.md)
Documentation for an example plugin. Use this when creating a new plugin.

[H3](../plugins/h3/README.md)
Documentation for the H3 plugin.

[JavaEmbed](../plugins/javaembed/javaembedOptions.md)
Options and configuration for the JavaEmbed plugin.

[JWT Security plugin](../system/security/plugins/jwtSecurity/README.md)
Documentation for JWT Security plugin.

[Kafka](../plugins/kafka/README.md)
Documentation for the Kafka plugin.

[Memcached](../plugins/memcached/README.md)
Documentation for the Memcached plugin.

[MongoDB](../plugins/mongodb/README.md)
Documentation for the MongoDB plugin.

[NLP](../plugins/nlp/README.md)
Documentation for the NLP plugin.

[Parquet](../plugins/parquet/README.md)
Documentation for the Parquet plugin.

[Redis](../plugins/redis/README.md)
Documentation for the Redis plugin.

[SQS](../plugins/sqs/README.md)
Documentation for the SQS plugin.
