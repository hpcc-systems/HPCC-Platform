# Developer Documentation

This is a collection of documentation by developers for developers.

## Introduction to Developer Documentation

Welcome to the Developer Documentation! This guide serves as a comprehensive resource for developers working with the HPCC Systems Platform. Here, you'll find detailed information about various components, tools, and services to help you build, maintain, and optimize your projects effectively.

### How to Use This Documentation

- **Explore Components**: Navigate through the sections below to learn about specific components of the HPCC Systems Platform.
- **Get Started Quickly**: Refer to the introductory guides and examples provided in each section.
- **Contribute**: If you have insights or improvements, feel free to contribute to this documentation.

### Developer/Contributor Guidelines

[Version Support](VersionSupport.md)
Details on Supported Versions

[Code Reviews](CodeReviews.md)
Best practices and guidelines for conducting code reviews.

[Code Submissions](CodeSubmissions.md)
Instructions for submitting code contributions to the repository.

[Development Workflow](Development.md)
Development workflow, testing, and environment setup instructions.

[Coding Style Guide](StyleGuide.md)
Coding style guide for HPCC Systems Platform development.

[Contributing Documentation](./docs/ContributeDocs.md)
Guide for contributing to HPCC Systems Platform documentation.

[HPCC Systems Documentation Style Guide](./docs/HPCCStyleGuide.md)
HPCC Systems Platform documentation style guide.

[Template for New Docs](./docs/DocTemplate.md)
Template for creating new documentation files.

[Authenticating with Git](GitAuthenticate.md)
Instructions for authenticating with Git in HPCC Systems development.

[Building Assets](UserBuildAssets.md)  
Instructions for building assets using GitHub Actions.

[Building Bare Metal Locally](../BUILD_ME.md)
Instructions for building the HPCC Systems Platform from source.

## General Platform Documents

[Platform Overview](../README.md) Overview of the HPCC Systems Platform.

[New Activities](newActivity.md)
Instructions for adding new activities to the platform.

[File Processing](NewFileProcessing.md)
Guide for implementing new file processing features.

[Memory Manager](MemoryManager.md) Documentation on memory management strategies.

[HTTP library](../system/httplib/README.md)
Documentation for the HTTP library in system components.

[Data Masking](../system/masking/include/readme.md)
Documentation for the data obfuscation framework.

[DataMasker plugin](../system/masking/plugins/datamasker/readme.md)
Documentation for DataMasker plugin.

[JWT Security plugin](../system/security/plugins/jwtSecurity/README.md)
Documentation for JWT Security plugin.

[Using vcpkg](../vcpkg/README.md)
Instructions for using vcpkg with the HPCC Systems Platform

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

## Sasha

Guidelines for handling the Sasha server, including maintenance and configuration.

[Sasha Readme](/dali/sasha/sasha.md)

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

## ECL Standard Library

The ECL Standard Library is a collection of pre-built functions, modules, and utilities that provide common functionality for ECL programming in the HPCC Systems Platform. It's designed to help developers write ECL code more efficiently by providing reusable components for common data processing tasks.

[Coding Style Guide](../ecllibrary/StyleGuide.md)
Style guide for ECL library code.

## ECL Watch

Instructions for ECL Watch.

[Coding and Contribution Instructions](../esp/src/.github/instructions/general.instructions.md)
General coding and contribution instructions for ECL Watch.

[React-specific Coding Instructions](../esp/src/.github/instructions/react-coding.instructions.md)
React-specific coding instructions for ECL Watch.

## ECLCC

Information on the ECL Compiler (ECLCC) and its operational aspects.

### Code Generator

The Code Generator is a powerful tool within the HPCC Systems Platform that automates the creation of C++ code from ECL Code, reducing development time and minimizing errors. Key aspects of the Code Generator: [Code Generator](CodeGenerator.md)

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

[LDAP Security](LDAPSecurityManager.md) Steps for configuring and managing LDAP for authentication and authorization.

### Secrets

Best practices for managing secrets securely within the system.

- [Kubernetes Secrets](../helm/examples/secrets/README-kubernetessecrets.md)

- [Vault Secrets K8s Authentication](../helm/examples/secrets/README-vault_secrets_using_kubernetes_authentication.md)

- [Vault Secrets Client Cert Vault Authentication](../helm/examples/secrets/README-vaultsecretsusingclientcertvaultauthentication.md)

## Thor

The Thor engine is the data refinery component of the HPCC Systems Platform, designed for high-performance, parallel data processing. It is responsible for executing complex data transformations and analytics at scale.

## Roxie

Roxie is the query processing engine, optimized for real-time data delivery. It is designed to handle high-concurrency, low-latency queries efficiently, making it ideal for queries requiring rapid responses.

[Roxie Technical Details](roxie.md)
Technical details and usage of the Roxie engine.

[Roxie FAQ](./userdoc/roxie/FAQ.md)  
Frequently asked questions about Roxie.

[Optimizing Roxie Query Performance](OptimizingRoxieQueryPerformance.md)
Explains how to configure Roxie to maintain consistent query response times by setting minimum execution thresholds through various methods including configuration files, ECL code options, and URL parameters. It also describes how to monitor performance.

[Memory Manager](MemoryManager.md) Documentation on memory management strategies.

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
