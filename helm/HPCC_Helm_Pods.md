# HPCC Systems® Helm Pod Guide

## Overview of HPCC Systems Helm Pods

This document describes the pod types created by the HPCC Systems® Helm chart, what each pod is used for, and how pod naming works.

The exact set of pods depends on the chart values that are enabled for a deployment. Some components are required for a functional HPCC Systems cluster, while others are optional and are only created when the corresponding feature is enabled.

## Scope of this guide

This guide covers the main workload pods created by the Helm chart under `helm/`. It focuses on steady-state workload pods and on short-lived job pods that may be created during installation or initialization.

It does not attempt to describe pods created by unrelated charts or by external Kubernetes add-ons.

## Pod naming conventions

Pod names are determined by the Kubernetes controller that creates them and by the Helm release name.

### StatefulSet pod names

StatefulSets create stable pod names with an ordinal suffix.

Pattern:

`<release-name>-<component-name>-<ordinal>`

Examples:

- `mycluster-dali-0`
- `mycluster-thor-1-0`

Use this pattern when the pod must keep a stable identity across restarts.

### Deployment pod names

Deployments create pod names that include a ReplicaSet hash and a random suffix.

Pattern:

`<release-name>-<component-name>-<replicaset-hash>-<pod-suffix>`

Example:

- `mycluster-eclwatch-7c9d8f6d79-q8l2m`

Use this pattern when the component is stateless and can be rescheduled freely.

### Job pod names

Kubernetes Jobs create short-lived pods for one-time work.

Pattern:

`<job-name>-<pod-suffix>`

Example:

- `mycluster-bootstrap-8x7k9`

These pods are temporary and are removed after the Job completes, subject to the cluster's retention settings.

### Name prefix rules

The leading release prefix comes from the Helm release name unless `fullnameOverride` or related naming settings change it. If those settings are used, the same pod naming rules still apply, but the prefix changes.

## Pod families created by the Helm chart

The following sections describe the common pod families created by the chart. The exact list of enabled pods depends on the selected values.

| Component | Typical pod count | What the pod is used for | Required or optional |
| --- | --- | --- | --- |
| Dali | 1 | Cluster metadata, file system metadata, and coordination services | Required for a functional cluster |
| DFU Server | 1 or more | File transfer and file management services | Optional for basic query clusters; required for DFU workflows |
| DaFileSrv | 1 or more per enabled instance | File server support for data access and storage integration | Optional |
| ECL Agent | 1 or more | Executes ECL workunits and supports distributed processing | Required when ECL execution is needed |
| ECL Compiler | 1 or more | Compiles ECL code into executable workunits | Required when compilation happens in-cluster |
| ECL Scheduler | 1 or more | Schedules and coordinates workunit execution | Optional |
| ESP / ECL Watch | 1 or more | Web user interface and HTTP API endpoints | Optional for headless clusters; commonly enabled |
| ESP service pods | 1 or more per enabled service | Hosts specific ESP services, such as query or management endpoints | Optional |
| Roxie | 1 or more | Real-time query serving | Required for Roxie deployments |
| Thor | 1 or more pods per Thor cluster | Batch processing, query execution, and distributed data processing | Required for Thor deployments |
| Sasha services | 1 or more per enabled service | Housekeeping, archiving, recovery, and retention tasks | Optional, but recommended |
| Bootstrap or setup Jobs | Temporary | One-time installation or initialization tasks | Optional, depending on chart features |

## Component details

### Dali

Dali is the metadata and coordination service for HPCC Systems. It stores cluster metadata (the System Data Store, or SDS) and supports consistent access to distributed resources.

- **Pod type:** StatefulSet.
- **Pod count:** One pod per Dali instance.
- **Usage:** Metadata management, coordination, and cluster state.
- **Requirement level:** Required for a functional cluster.

### DFU Server

The DFU Server provides file transfer and file management services (Distributed File Services, or DFS).

- **Pod type:** Usually a Deployment.
- **Pod count:** One or more replicas, depending on the chart values.
- **Usage:** Upload, download, and file movement operations.
- **Requirement level:** Optional for basic query-only deployments, but required when DFU operations are needed.

### DaFileSrv

DaFileSrv supports file access and storage integration.

- **Pod type:** Usually a Deployment.
- **Pod count:** One or more pods, depending on the enabled storage configuration.
- **Usage:** Storage access and file-serving support.
- **Requirement level:** Optional.

### ECL Agent

The ECL Agent executes ECL workunits.

- **Pod type:** Usually a Deployment.
- **Pod count:** One or more replicas.
- **Usage:** Workunit execution and distributed processing support.
- **Requirement level:** Required when the cluster runs ECL workloads.

### ECL Compiler

The ECL Compiler compiles ECL source into executable workunits.

- **Pod type:** Usually a Deployment.
- **Pod count:** One or more replicas.
- **Usage:** Compilation of ECL code.
- **Requirement level:** Required when compilation is performed in the cluster.

### ECL Scheduler

The ECL Scheduler coordinates workunit scheduling.

- **Pod type:** Usually a Deployment.
- **Pod count:** One or more replicas.
- **Usage:** Scheduling and queue management for ECL execution.
- **Requirement level:** Optional.

### ESP and ECL Watch

ESP provides web and API services, including ECL Watch.

- **Pod type:** Usually a Deployment.
- **Pod count:** One or more replicas.
- **Usage:** Browser-based management, REST APIs, and user interaction.
- **Requirement level:** Optional for automated clusters, but commonly enabled for administration.

### ESP service pods

Some ESP services are deployed as separate pods.

- **Pod type:** Usually a Deployment.
- **Pod count:** One or more replicas per enabled service.
- **Usage:** Specific HTTP or management services, depending on the enabled chart options.
- **Requirement level:** Optional.

### Roxie

Roxie provides real-time query serving. It is commonly known as the Rapid Data Delivery Engine.

- **Pod type:** Typically a set of workload pods managed by Kubernetes.
- **Pod count:** One or more pods per Roxie cluster.
- **Usage:** Low-latency query handling.
- **Requirement level:** Required for Roxie deployments, optional otherwise.
- **Multiple pods:** A Roxie cluster commonly uses multiple pods so queries can be distributed across the cluster.

### Thor

Thor provides batch processing and large-scale distributed query execution. It is commonly known as the Data Factory.

- **Pod type:** Typically a set of workload pods managed by Kubernetes.
- **Pod count:** One or more pods per Thor cluster.
- **Usage:** Batch processing and distributed computation.
- **Requirement level:** Required for Thor deployments, optional otherwise.
- **Multiple pods:** A Thor cluster usually includes a master pod and one or more worker pods.

### Sasha services

Sasha services perform background housekeeping tasks. It is commonly known as Dali's assistant.

- **Pod type:** Usually a Deployment.
- **Pod count:** One or more pods per enabled Sasha service.
- **Usage:** Archiving, recovery, retention, or related maintenance tasks.
- **Requirement level:** Optional, but recommended.
- **Multiple pods:** Each enabled Sasha subservice can create its own pod set.

### Bootstrap or setup Jobs

The chart may create short-lived Jobs for initialization tasks.

- **Pod type:** Job.
- **Pod count:** Temporary, created only when the Job runs.
- **Usage:** Bootstrap, initialization, or other one-time setup work.
- **Requirement level:** Optional and feature-dependent.

## Required versus optional components

A component is treated as required in this guide when a normal cluster deployment depends on it for core functionality.

A component is treated as optional when the chart can be installed without it and the cluster can still function for a narrower use case.

Examples:

- Dali is required for cluster coordination.
- Roxie is required only when a Roxie serving cluster is deployed.
- Thor is required only when batch processing is deployed.
- ECL Watch is optional for headless deployments, but useful for administration.
- Sasha services are optional background services.

## Multiple-pod components

Some components create more than one pod.

- **Roxie:** Multiple peer pods, typically one per configured Roxie server.
- **Thor:** One master pod plus one or more worker pods.
- **ECL Agent, ECL Compiler, DFU Server, ESP, and Sasha services:** May all scale to multiple replicas if the chart values enable that behavior.

When a component scales out, the pod name pattern depends on the controller type. StatefulSets use stable ordinals, while Deployments use ReplicaSet-generated suffixes.

## Practical summary

If you are trying to identify a pod from an HPCC Systems Helm deployment:

1. Check whether the pod name ends with an ordinal, such as `-0` or `-1`. If it does, the pod is usually managed by a StatefulSet.
2. Check whether the pod name includes a ReplicaSet hash and a random suffix. If it does, the pod is usually managed by a Deployment.
3. Check whether the pod is short-lived and tied to setup work. If it is, it is probably a Job pod.
4. Use the component name in the middle of the pod name to identify the workload, such as `dali`, `thor`, `roxie`, or `eclwatch`.

## Notes

The exact pod set is controlled by the chart values file and by any overrides supplied at install time. If you need a deployment-specific inventory, compare the enabled values against the templates under `helm/` and the rendered manifests from `helm template`.
