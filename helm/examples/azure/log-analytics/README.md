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

#### b - Execute enable-loganalytics.sh

This helper script attempts to create new Azure LogAnalytics workspace (user can provide pre-existing), associates the workspace with the target AKS cluster, and enables the Azure Log Analytics feature.This script is dependant on the values provided in the previous step

### 2 - Configure HPCC logAccess
The logAccess feature allows HPCC to query and package relevant logs for various features such as ZAP report, WorkUnit helper logs, ECLWatch log viewer, etc.

#### a - Procure AAD registered application
Azure requires an Azure Active Directory registered application in order to broker Log Analytics API access. See official documentation:
https://docs.microsoft.com/en-us/power-apps/developer/data-platform/walkthrough-register-app-azure-active-directory

Depending on your Azure subscription structure, it might be necessary to request this from a subscription administrator.

#### b - Provide AAD registered application inforation
HPCC logAccess requires access to the AAD Tenant, client, token, and target workspace ID via secure secret object.
The secret is expected to be in the 'esp' category, and be named 'azure-logaccess'.
The following kv pairs are supported
- aad-tenant-id
- aad-client-id
- aad-client-secret
- ala-workspace-id

The included 'create-azure-logaccess-secret.sh' helper can be used to create the necessary secret
Example manual secret creation command (assuming ./secrets-templates contains a file named exactly as the above keys):
```console
  create-azure-logaccess-secret.sh .HPCC-Platform/helm/examples/azure/log-analytics/secrets-templates/
```

Otherwise, users can create the secret manually.
Example manual secret creation command (assuming ./secrets-templates contains a file named exactly as the above keys):
```console
  kubectl create secret generic azure-logaccess --from-file=HPCC-Platform/helm/examples/azure/log-analytics/secrets-templates/
```

#### c - Configure HPCC logAccess
The target HPCC deployment should be directed to use the above Azure Log Analytics workspace, and the newly created secret by providing appropriate logAccess values (such as ./loganalytics-hpcc-logaccess.yaml). 

Example use:
```console
  helm install myhpcc hpcc/hpcc -f HPCC-Platform/helm/examples/azure/log-analytics/loganalytics-hpcc-logaccess.yaml
```

