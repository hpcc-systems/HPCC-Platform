# Identity Management Integration

## Overview

The HPCC Helm chart supports integration with cloud provider identity management systems through ServiceAccount annotations and pod labels. This enables secure, credential-free access to cloud resources using workload identity features.

## ServiceAccount Configuration

The HPCC platform creates several ServiceAccounts with different permission levels:

- **hpcc-default**: Used by components that don't need API access to launch child jobs
- **hpcc-agent**: Used by components that need API access to launch child jobs (e.g., eclagent, eclccserver when not using child processes)
- **hpcc-thoragent**: Used by Thor manager for launching child jobs and managing NetworkPolicies
- **hpcc-esp-service**: Used by ESP components that need to check service status
- **hpcc-dali**: Used by Dali components to find external directio dafilesrv services

### Adding Annotations and Labels

You can configure annotations and pod labels for each ServiceAccount through the `global.serviceAccounts` section in your values file:

```yaml
global:
  serviceAccounts:
    <service-account-name>:
      annotations:
        <key>: <value>
      podLabels:
        <key>: <value>
```

- **annotations**: Added to the ServiceAccount resource itself
- **podLabels**: Added to all pods that use this ServiceAccount

## Azure Workload Identity

Azure Workload Identity allows pods to authenticate to Azure services using managed identities without storing credentials in Kubernetes secrets.

### Prerequisites

1. An Azure Kubernetes Service (AKS) cluster with Workload Identity enabled
2. Azure managed identities created for your HPCC components
3. Federated identity credentials configured for the managed identities

### Configuration Example

```yaml
global:
  serviceAccounts:
    default:
      annotations:
        azure.workload.identity/client-id: "00000000-0000-0000-0000-000000000000"
      podLabels:
        azure.workload.identity/use: "true"

    agent:
      annotations:
        azure.workload.identity/client-id: "11111111-1111-1111-1111-111111111111"
      podLabels:
        azure.workload.identity/use: "true"

    thoragent:
      annotations:
        azure.workload.identity/client-id: "22222222-2222-2222-2222-222222222222"
      podLabels:
        azure.workload.identity/use: "true"

    esp-service:
      annotations:
        azure.workload.identity/client-id: "33333333-3333-3333-3333-333333333333"
      podLabels:
        azure.workload.identity/use: "true"

    dali:
      annotations:
        azure.workload.identity/client-id: "44444444-4444-4444-4444-444444444444"
      podLabels:
        azure.workload.identity/use: "true"
```
