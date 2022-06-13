# HPCC Azure Log Analytics

Azure Log Analytics provides powerful log processing and monitoring tools.
It can be leveraged to process HPCC component logs by associating an HPCC Systems hosting AKS cluster with 
a Log Analytics workspace, and explicitly enabling the Log Analytics feature. A new or a pre-existing workspace can be used for this purpose. This example includes a script which can create an Azure Log Analytics workspace (or specify a pre-existing workspace), link it to the AKS cluster hosting HPCC components, and enable Log Analytics.

HPCC has several components which fetch HPCC component logs via the HPCC LogAccess framework, which has to be configured to access the Log Analytics workspace. The values yaml file provided here should be used as part of a Helm based HPCC deployment.

## enable-loganalytics.sh

This helper script enables the Azure Log Analytics feature on a target AKS cluster (which hosts HPCC) creates a new Azure LogAnalytics workspace (user can provide pre-existing )
The workspace is then associated with the target AKS cluster on which HPCC is deployed.

This script is dependant on several environment variables which are defined in ./env-loganalytics

## env-loganalytics
Declares several environment variables needed to create an Azure LogAnalytics workspace.
The user should populate the following values before executing the ./enable-loganalytics.sh script:

LOGANALYTICS_RESOURCE_GROUP - The Azure resource group associated with the target AKS cluster
                            - The new workspace will be associated with this resource group

LOGANALYTICS_WORKSPACE_NAME - The desired name for the Azure LogAnalytics workspace to be created

TAGS - The tags associated with the new workspace
     - For example: "admin=MyName email=my.email@mycompany.com environment=myenv justification=testing"

AKS_CLUSTER_NAME - Name of the AKS cluster to associate newly created log analytics workspace

AZURE_SUBSCRIPTION - Optional - Ensures this subscription is set before creating the new workspace

## loganalytics-hpcc-logaccess.yaml

This is a values file that can be supplied to Helm when starting HPCC.
It will direct the Log Access framework to target Azure Log Analytics

This means functionality which fetches HPCC component logs will attempt to gather logs via KQL queries.

Example use:
```console
  helm install myhpcc hpcc/hpcc -f HPCC-Platform/helm/examples/azure/log-analytics/loganalytics-hpcc-logaccess.yaml
```
