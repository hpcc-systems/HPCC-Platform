#!/bin/bash
WORK_DIR=$(dirname $0)
source ${WORK_DIR}/env-loganalytics

if [[ -z "$LOGANALYTICS_WORKSPACE_NAME" ]]
then
   echo "Missing LOGANALYTICS_WORKSPACE_NAME environment variable - this should be set to the desired name of the new Azure LogAnalytics workspace"
   exit 1
fi

if [[ -z "$LOGANALYTICS_RESOURCE_GROUP" ]]
then
   echo "Missing LOGANALYTICS_RESOURCE_GROUP environment variable - this should be set to the Azure resource group associated with the target AKS cluster"
   exit 1
fi

if [[ -z "$AKS_CLUSTER_NAME" ]]
then
   echo "Missing AKS_CLUSTER_NAME environment variable - this should be set to the AKS cluster to be monitored via newly created LogAnalytics workspace"
   exit 1
fi
 
if [[ -n "$AZURE_SUBSCRIPTION" ]]
then
   echo "Setting subscription to '$AZURE_SUBSCRIPTION'..."
   az account set --subscription $AZURE_SUBSCRIPTION
fi

tid=$(az account show --query tenantId)
if [[ -z "$tid" ]]
then
   echo "Could not determine Azure Subscription Tenant ID!"
else
   echo "Tenant ID: ${tid}"
fi

wscid=$(az monitor log-analytics workspace show -g $LOGANALYTICS_RESOURCE_GROUP -n $LOGANALYTICS_WORKSPACE_NAME --query customerId)
if [[ -z "$wscid" ]]
then
  echo "Creating workspace..."
  wsid=$(az monitor log-analytics workspace create -g $LOGANALYTICS_RESOURCE_GROUP -n $LOGANALYTICS_WORKSPACE_NAME --query-access Enabled --tags $TAGS --query id)
  if [[ $? -ne 0 ]] || [[ -z "$wsid" ]]
  then
    echo "Could not create target log-analytics workspace '${LOGANALYTICS_RESOURCE_GROUP}'!"
    exit 1
  else
    echo "Success, workspace id: '$wsid'"
    echo "Fetching workspace custumerID..."
    wscid=$(az monitor log-analytics workspace show -g $LOGANALYTICS_RESOURCE_GROUP -n $LOGANALYTICS_WORKSPACE_NAME --query customerId)
    if [[ $? -ne 0 ]] || [[ -z "$wsid" ]]
    then
      echo "Could not create target log-analytics workspace '${LOGANALYTICS_RESOURCE_GROUP}'!"
    else
      echo "Success, workspace customer id: '$wscid' (used as workspace id)"
    fi
  fi
else
  echo "Azure LogAnalytics workspace '${LOGANALYTICS_RESOURCE_GROUP}' already exists!"
fi

echo "Enabling workspace on target AKS cluster '$AKS_CLUSTER_NAME'..."
az aks enable-addons -g $LOGANALYTICS_RESOURCE_GROUP -n $AKS_CLUSTER_NAME -a monitoring --workspace-resource-id "$wsid"
if [[ $? -ne 0 ]]
then
  echo "Could not enable target log-analytics workspace '${LOGANALYTICS_RESOURCE_GROUP}'!"
  exit 1
else
  echo "Success, workspace id: '$wsid' enabled on AKS $AKS_CLUSTER_NAME"
fi
