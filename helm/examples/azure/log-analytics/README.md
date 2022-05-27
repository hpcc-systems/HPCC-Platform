# HPCC Azure Log Analytics

## Directory contents

### create-workspace.sh

This helper script creates a new Azure LogAnalytics workspace (necessary for processing logs).
The workspace is then associated with the target AKS cluster on which HPCC is deployed.

This script is dependant on several environment variables which are defined in ./env-loganalytics

### env-loganalytics
Declares several environment variables needed to create an Azure LogAnalytics workspace.
The user should populate the following values before executing the ./create-workspace.sh script:

LOGANALYTICS_RESOURCE_GROUP - The Azure resource group associated with the target AKS cluster
                            - The new workspace will be associated with this resource group

LOGANALYTICS_WORKSPACE_NAME - The desired name for the Azure LogAnalytics workspace to be created

TAGS - The tags associated with the new workspace
     - For example: "admin=MyName email=my.email@mycompany.com environment=myenv justification=testing"

AKS_CLUSTER_NAME - Name of the AKS cluster to associate newly created log analytics workspace

AZURE_SUBSCRIPTION - Optiona - Ensures this subscription is set before creating the new workspace

### loganalytics-hpcc-logaccess.yaml

This is a values file that can be supplied to Helm when starting HPCC.
It will direct the Log Access framework to target Azure Log Analytics

This means functionality which fetches HPCC component logs will attempt to gather logs via KQL queries.

Example use:
```console
  helm install myhpcc hpcc/hpcc -f HPCC-Platform/helm/examples/azure/log-analytics/loganalytics-hpcc-logaccess.yaml
```

