# Developer Documentation

This is a collection of documentation by developers for developers.

## Introduction

Welcome to the Developer Documentation! This guide serves as a comprehensive resource for developers working with the HPCC Platform. Here, you'll find detailed information about various components, tools, and services to help you build, maintain, and optimize your projects effectively.

## How to Use This Documentation

- **Explore Components**: Navigate through the sections below to learn about specific components of the HPCC Platform.
- **Get Started Quickly**: Refer to the introductory guides and examples provided in each section.
- **Contribute**: If you have insights or improvements, feel free to contribute to this documentation.

## Platform Versions

Details on Supported Versions

[Version Support](VersionSupport.md)

## Dali

Dali is the central metadata repository and coordination service in the HPCC Platform. It plays a critical role in managing and storing system metadata, including file information, cluster configurations, and workunit details. Dali ensures consistency and synchronization across the platform, enabling efficient resource management and job execution.

## Sasha

Guidelines for handling the Sasha server, including maintenance and configuration.

[Sasha Readme](/dali/sasha/sasha.md)

## DFU

The Distributed File Utility (DFU) is a core component of the HPCC Platform that facilitates the management of files across the distributed environment. It provides tools for transferring, replicating, and managing data efficiently. DFU ensures data integrity and supports operations such as file copy, delete, spray (import), and despray (export).

### Key Features

- **File Transfer (Copy/Remote Copy)**: Move files between nodes or clusters seamlessly.
- **Data Spray**: Import large datasets into the HPCC Platform for processing.
- **Data Despray**: Export processed data from the platform to external systems.
- **Replication**: Ensure data redundancy and availability across the cluster.
- **File Management**: Perform operations like renaming, deleting, or modifying file attributes.
- **Superfile Management**: Perform superfile operations like adding subfiles, deleting subfiles, or modifying file attributes.

[//]: # (TO DO: Add link)

## DFUPlus

DFUPlus is a command-line utility that provides advanced file management capabilities within the HPCC Platform. It is designed to interact with the Distributed File Utility (DFU) service, enabling users to perform file operations programmatically or through scripts.

[//]: # (TO DO: Add link)

## ECL Watch

Instructions for monitoring and managing the system using ECL Watch.

[//]: # (TO DO)

## ECLCC

Information on the ECL Compiler (ECL CC) and its operational aspects.

### Code Generator

The Code Generator is a powerful tool within the HPCC Platform that automates the creation of C++ code from ECL Code, reducing development time and minimizing errors. Key aspects of the Code Generator: [Code Generator](CodeGenerator.md)

## ECL Agent / hThor

Operational details for ECL Agent and hThor, including job execution and monitoring.

## EVTool

Documentation on using EVTool for system diagnostics and analysis.

## FTSlave

Guidelines for configuring and managing FTSlave services.

## ESDL

Comprehensive instructions for working with Enterprise Services Definition Language (ESDL) and the ESDL Tools.

For more details about the CLI, refer to the [ESDL Command Line Tool](../tools/esdlcmd/README.md)

## Security

- [Security User Authentication](SecurityUserAuthentication.md)
- [Security Configuration](SecurityConfig.md)

### LDAP

Steps for configuring and managing LDAP for authentication and authorization. [LDAP Security](LDAPSecurityManager.md)

### Secrets

Best practices for managing secrets securely within the system.

- [Kubernetes Secrets](../helm/examples/secrets/README-kubernetessecrets.md)

- [Vault Secrets K8s Authentication](../helm/examples/secrets/README-vault_secrets_using_kubernetes_authentication.md)

- [Vault Secrets Client Cert Vault Authentication](../helm/examples/secrets/README-vaultsecretsusingclientcertvaultauthentication.md)

## Workunits

In HPCC Systems, workunits are central to the execution of ECL (Enterprise Control Language) queries. Each query submitted to the system generates a unique workunit ID (WUID), which can be used to track and manage the job. It represents the lifecycle of a job from submission to completion, including all the metadata, inputs, outputs, and logs associated with it.
For an explanation of workunits, and a walk-through of the steps in executing a query, see:
[WorkunitWorkflow](Workunits.md)

## Thor

The Thor engine is the data refinery component of the HPCC Platform, designed for high-performance, parallel data processing. It is responsible for executing complex data transformations and analytics at scale.

[//]: # (TO DO)

## Roxie

Roxie is the query processing engine, optimized for real-time data delivery. It is designed to handle high-concurrency, low-latency queries efficiently, making it ideal for queries requiring rapid responses.

### Roxie Memory Manager

[MemoryManager](MemoryManager.md)

## Metrics Framework

The Metrics Framework in HPCC Systems is a system designed to collect, manage, and report metrics across various components of the platform. Metrics are quantitative measurements that provide insights into the performance, resource usage, and operational health of the system.

For more details, you can refer to the linked Metrics Framework Design in your documentation. [Metrics Framework Design](Metrics.md)

## Logging

Logging in the HPCC Platform provides a robust mechanism for tracking system activities, diagnosing issues, and monitoring performance. Logs are generated by various components and services, offering detailed insights into operations, errors, and events. Developers can use these logs to troubleshoot problems, optimize performance, and ensure system reliability.
For more details, you can refer to the linked Metrics Framework Design in your documentation.

[//]: # (TO DO)

## Regression Suite

The Regression Suite in the HPCC Platform is a comprehensive testing framework designed to ensure the stability, reliability, and correctness of the system. It consists of a collection of automated tests that validate the functionality of various components and features across different platform versions.

### Regression Suite Key Features

- **Automated Testing**: Execute a wide range of tests automatically to verify system behavior.
- **Version Compatibility**: Ensure that new changes do not break compatibility with previous versions.
- **Extensibility**: Add custom tests to cover specific use cases or new features.
- **Detailed Reporting**: Generate reports to analyze test results and identify issues.

[//]: # (TO DO:add link)
