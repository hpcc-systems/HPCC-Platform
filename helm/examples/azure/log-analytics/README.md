# HPCC Azure Log Analytics

Azure Log Analytics provides powerful log processing and monitoring tools.
It can be leveraged to process HPCC component logs by associating an HPCC Systems hosting AKS cluster with 
a Log Analytics workspace, and explicitly enabling the Log Analytics feature. A new or a pre-existing workspace can be used for this purpose. This example includes a script which can create an Azure Log Analytics workspace (or specify a pre-existing workspace), link it to the AKS cluster hosting HPCC components, and enable Log Analytics.

HPCC has several components which fetch HPCC component logs via the HPCC LogAccess framework, which has to be configured to access the Log Analytics workspace. The values yaml file provided here should be used as part of a Helm based HPCC deployment.

## Quickstart
### 1 - Enabling Log Analytics
Once these two steps are completed, HPCC component logs should be routed to Azure Log Analytics and accessible via the Portal
#### a - Provide required information in env-loganalytics
The user should populate the following values in order to create a new Azure Log Analytics workspace, associate it with a target AKS cluster, and enable to processing of logs

- LOGANALYTICS_WORKSPACE_NAME (Desired name for the Azure LogAnalytics workspace to be associated with target AKS cluster)
 New workspace will be created if it does not exist
- LOGANALYTICS_RESOURCE_GROUP (The Azure resource group associated with the target AKS cluster)
 New workspace will be associated with this resource group
- AKS_CLUSTER_NAME (Name of the target AKS cluster to associate log analytics workspace)
- AKS_RESOURCE_GROUP (Azure resource group associated with the target AKS cluster)
- TAGS - The tags associated with the new workspace
     For example: "admin=MyName email=my.email@mycompany.com environment=myenv justification=testing"
- AZURE_SUBSCRIPTION (Optional - Ensures this subscription is set before creating the new workspace)

- AKS_RESOURCE_LOCATION (Optional e.g. eastus)

- ENABLE_CONTAINER_LOG_V2 (true|false) Enables the ContainerLog V2 schema.
   If set to true, the stdout/stderr Logs are forwarded to ContainerLogV2 table, otherwise the container logs continue to be forwarded to ContainerLog table.
   Utilizes ./helm/examples/azure/log-analytics/dataCollectionSettings.json, and 
   ./helm/examples/azure/log-analytics/container-azm-ms-agentconfig.yaml which creates and applies a new configmap 'kube-system/container-azm-ms-agentconfig'.

   Details on benefits of V2 schema: https://learn.microsoft.com/en-us/azure/azure-monitor/containers/container-insights-logging-v2?tabs=configure-portal


#### b - Execute enable-loganalytics.sh

This helper script attempts to create new Azure LogAnalytics workspace (user can provide pre-existing), associates the workspace with the target AKS cluster, and enables the Azure Log Analytics feature. This script is dependant on the values provided in the previous step.

### 2 - Configure HPCC logAccess
The logAccess feature allows HPCC to query and package relevant logs for various features such as ZAP report, WorkUnit helper logs, ECLWatch log viewer, etc.

#### a - Procure AAD registered application
Azure requires an Azure Active Directory registered application in order to broker Log Analytics API access. The registered application should be assigned Log Analytics roles. See official documentation:
https://docs.microsoft.com/en-us/power-apps/developer/data-platform/walkthrough-register-app-azure-active-directory

Depending on your Azure subscription structure, it might be necessary to request this from a tenant/subscription administrator.

The Registered Application must provide a 'client secret' which is used to gain access to the Log Analytics API.

#### b - Provide AAD registered application information and target ALA Workspace 
HPCC logAccess requires access to the AAD Tenant ID, client ID, and secret which are provided by the registered app from section '2.a' above. The target workspace ID is also required, and can be retrieved after the step in section '1.b' is successfully completed. Those four values must be provided via a secure secret object.

The secret is expected to be in the 'esp' category, and be named 'azure-logaccess'.
The following key-value pairs are required (key names must be spelled exactly as shown here)
- **aad-tenant-id** - This should contain the Tenant ID of the AAD registered application
- **aad-client-id** - This is the AAD registered application ID (Ensure it has Log Analytics access roles)
- **aad-client-secret** - This is a secret provided by the AAD registered app for Log Analytics access
- **ala-workspace-id** - The ID of the Azure Log Analytics workspace which contains the HPCC component logs

The included 'create-azure-logaccess-secret.sh' helper can be used to create the necessary secret.

Example scripted secret creation command (assuming ./secrets-templates contains a file named exactly as the above keys):
```console
  create-azure-logaccess-secret.sh .HPCC-Platform/helm/examples/azure/log-analytics/secrets-templates/
```

Otherwise, users can create the secret manually.

Example manual secret creation command (assuming ./secrets-templates contains a file named exactly as the above keys):
```console
  kubectl create secret generic azure-logaccess --from-file=HPCC-Platform/helm/examples/azure/log-analytics/secrets-templates/
```

#### c - Configure HPCC logAccess
The target HPCC deployment should be directed to use the above Azure Log Analytics workspace, and the newly created secret by providing appropriate logAccess values (such as ./loganalytics-hpcc-logaccess.yaml or ./loganalytics-hpcc-logaccessV2.yaml if targeting Azure Log Analytics ContainerLogV2 - recommended ). 

Example use:
```console
  helm install myhpcc hpcc/hpcc -f HPCC-Platform/helm/examples/azure/log-analytics/loganalytics-hpcc-logaccessV2.yaml
```
## Directory Contents

- 'create-azure-logaccess-secret.sh' - Script for creating 'azure-logaccess' secret needed for accessing logs stored in Azure Log Analytics
- 'secrets-templates' - Contains placeholders for information required to create 'azure-logaccess' secret via 'create-azure-logaccess-secret.sh' script
- 'enable-loganalytics.sh' - Script for enabling Azure LogAnalytics upon a given AKS cluster
- 'env-loganalytics' - Environment information required to enable ALA upon target AKS cluster.
- 'dataCollectionSettings.json' - Provided to enable ContainerLogV2 schema on target AKS cluster
- 'container-azm-ms-agentconfig.yaml' - Defines ConfigMap used to configure ALA log collection. Provided to re-direct ALA log collection to ContainerLogV2 schema.
- 'loganalytics-hpcc-logaccess.yaml' - Used to configure ALA -> HPCC LogAccess. Provides mapping between ALA log tables to HPCC's known log categories
- 'loganalytics-hpcc-logaccessV2.yaml' - Used to configure ALA -> HPCC LogAccess. Provides mapping between ALA ContainerLogV2 log table to HPCC's known log categories
