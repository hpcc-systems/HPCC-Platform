# Operational Documentation

This document serves as a comprehensive guide for system administrators, providing insights and instructions from developers to ensure smooth operations.

## Dali

Documentation for managing and troubleshooting the Dali server.

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

Advanced features and usage of DFUPlus for enhanced file management.

## ECL Watch

Instructions for monitoring and managing the system using ECL Watch.

## ECLCC

Information on the ECL Compiler (ECL CC) and its operational aspects.

## ECL Agent / hThor

Operational details for ECL Agent and hThor, including job execution and monitoring.

## EVTool

Documentation on using EVTool for system diagnostics and analysis.

## FTSlave

Guidelines for configuring and managing FTSlave services.

## ESDL

Comprehensive instructions for working with Enterprise Services Definition Language (ESDL).

## Security

### LDAP

Steps for configuring and managing LDAP for authentication and authorization.

### Secrets

Documentation on managing sensitive information such as passwords, API keys, and certificates. This includes:

- **Storage**: Guidelines for securely storing secrets using tools like HashiCorp Vault, AWS Secrets Manager, or Azure Key Vault.
- **Access Control**: Best practices for restricting access to secrets based on roles and permissions.
- **Rotation**: Steps for regularly rotating secrets to minimize security risks.
- **Encryption**: Recommendations for encrypting secrets both at rest and in transit.
- **Auditing**: Instructions for monitoring and auditing access to secrets to ensure compliance and detect unauthorized access.
- **Environment Variables**: Tips for securely managing secrets in environment variables for applications.
- **Backup and Recovery**: Procedures for securely backing up and recovering secrets in case of data loss.

## Roxie

Operational guidelines for managing the Roxie query cluster, including deployment, query optimization, and troubleshooting.

### [OptimizingRoxieQueryPerformance](OptimizingRoxieQueryPerformance.md)

Explains how to configure Roxie to maintain consistent query response times by setting minimum execution thresholds through various methods including configuration files, ECL code options, and URL parameters. It also describes how to monitor performance.

## Thor

Instructions for configuring and maintaining the Thor data processing engine, with details on workload distribution, performance tuning, and fault tolerance.
