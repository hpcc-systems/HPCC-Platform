# End User Documentation

Welcome to the End User Documentation. This guide provides a comprehensive collection of resources and instructions created by developers to assist end-users in effectively utilizing the platform.

## General HPCC Platform Documents

[Version Support](VersionSupport.md)
Details on Supported Versions

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

## ECL Bundles

ECL Bundles are packages of ECL (Enterprise Control Language) code that can be distributed, shared, and reused across different HPCC Systems Platform installations. They provide a way to package ECL modules, functions, and related resources into a distributable format.

[ECL Bundles](../ecl/ecl-bundle/DOCUMENTATION.md)  
Documentation for ECL bundle features and usage.

[ECL Bundles Repository](https://github.com/hpcc-systems/ecl-bundles)
The repository that serves as a central list of all known ECL bundles.

## Copilot

Details about using Copilot for work on the HPCC Systems platform.

[Prompt Tips](./userdoc/copilot/CopilotPromptTips.md)
Tips for writing effective prompts for GitHub Copilot.

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

## DFU

The Distributed File Utility (DFU) is a core component of the HPCC Systems Platform that facilitates the management of files across the distributed environment. It provides tools for transferring, replicating, and managing data efficiently. DFU ensures data integrity and supports operations such as file copy, delete, spray (import), and despray (export).

### Key Features of DFU

- **File Transfer (Copy/Remote Copy)**: Move files between nodes or clusters seamlessly.
- **Data Spray**: Import large datasets into the HPCC Systems Platform for processing.
- **Data Despray**: Export processed data from the platform to external systems.
- **Replication**: Ensure data redundancy and availability across the cluster.
- **File Management**: Perform operations like renaming, deleting, or modifying file attributes.
- **Superfile Management**: Perform superfile operations like adding subfiles, deleting subfiles, or modifying file attributes.

## DFUPlus

DFUPlus is a command-line utility that provides advanced file management capabilities within the HPCC Systems Platform. It is designed to interact with the Distributed File Utility (DFU) service, enabling users to perform file operations programmatically or through scripts.

[Client Tools Documentation](https://hpccsystems.com/training/documentation/ecl-ide-and-client-tools/)

## ECL

### ECL Language Reference

ECL is the Enterprise Control Language designed specifically for huge data projects using the HPCC Systems platform. Its extreme scalability comes from a design that allows you to leverage every query you create for re-use in subsequent queries as needed.

[ECL Language Reference](https://hpccsystems.com/wp-content/uploads/_documents/ECLR_EN_US/index.html) Online Access to the ECL Language Reference.

### ECL Standard Library

The ECL Standard Library is a collection of pre-built functions, modules, and utilities that provide common functionality for ECL programming in the HPCC Systems Platform. It's designed to help developers write ECL code more efficiently by providing reusable components for common data processing tasks.

[ECL Standard Library Reference](https://hpccsystems.com/wp-content/uploads/_documents/SLR_EN_US/index.html) Online Access to the ECL Standard Library Reference.

### ECL IDE

The ECL IDE is the simple and easy way to create Queries into your data, and ECL files with which to build your queries.

[Client Tools Documentation](https://hpccsystems.com/training/documentation/ecl-ide-and-client-tools/)

### ECL Language Extension for Visual Studio Code

This extension adds rich language support for HPCC Systems ECL language for the HPCC-Platform) to VS Code.

[ECL Extension on Visual Studio Marketplace](https://marketplace.visualstudio.com/items?itemName=hpcc-systems.ecl)

## ECL Watch

## ECLCC

## Embedded Languages

### Java

[Options for Embedded Java in HPCC](../plugins/javaembed/javaembedOptions.md)
This article describes configuration options for the embedded Java plugin in HPCC Platform, which allows ECL code to execute Java functionality.

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

[Security User Authentication](SecurityUserAuthentication.md) User authentication mechanisms and configuration.

[Security Configuration](SecurityConfig.md) Configuration options for HPCC Systems Platform security.

[JWT Security plugin](../system/security/plugins/jwtSecurity/README.md) Documentation for JWT Security plugin.

### LDAP

### Secrets

Best practices for managing secrets securely within the system.

- [Kubernetes Secrets](../helm/examples/secrets/README-kubernetessecrets.md)
Covers the native Kubernetes approach to secrets management.

- [Vault Secrets K8s Authentication](../helm/examples/secrets/README-vault_secrets_using_kubernetes_authentication.md)
Documents integration with HashiCorp Vault using Kubernetes-based authentication.

- [Vault Secrets Client Cert Vault Authentication](../helm/examples/secrets/README-vaultsecretsusingclientcertvaultauthentication.md)
Covers Vault integration using client certificate authentication.

## Roxie

Roxie is the query processing engine, optimized for real-time data delivery. It is designed to handle high-concurrency, low-latency queries efficiently, making it ideal for queries requiring rapid responses.

[Roxie Technical Details](roxie.md) Technical details and usage of the Roxie engine.

[Roxie FAQ](./userdoc/roxie/FAQ.md) Frequently asked questions about Roxie.

[Optimizing Roxie Query Performance](OptimizingRoxieQueryPerformance.md) Explains how to configure Roxie to maintain consistent query response times by setting minimum execution thresholds through various methods including configuration files, ECL code options, and URL parameters. It also describes how to monitor performance.

[Memory Manager](MemoryManager.md) Documentation on memory management strategies.

## Thor

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
Documentation for the H3 plugin which exposes the H3 library (a hexagonal hierarchical geospatial indexing system) to ECL.

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
