#!/bin/bash
WORK_DIR=$(dirname $0)
source ${WORK_DIR}/env-sa

[[ -n "$SUBSCRIPTION" ]] && az account set --subscription $SUBSCRIPTION

# Get AKS Resource Group
if [ -z "$AKS_RESOURCE_GROUP" ]
then
  if [ -z "$RESOURCE_GROUP" ] || [ -z "$AKS_NAME" ]
  then
    echo "Resource group: $RESOURCE_GROUP, AKS name: $AKS_NAME. Need both resource group and AKS name to query AKS resource group"
    exit 1
  fi
  echo "Query AKS resource group"
  AKS_RESOURCE_GROUP=$(az aks show -g $RESOURCE_GROUP -n $AKS_NAME 2>&1 | \
	  grep "^[[:space:]]\+ \"resourceGroup\"" | \
	  grep -v "\"${RESOURCE_GROUP}\"" | \
	  cut -d':' -f 2 | \
	  cut -d'"' -f 2)
fi

echo "Set default network access rule to Deny"
az storage account update --resource-group $SA_RESOURCE_GROUP --name $STORAGE_ACCOUNT_NAME --default-action Deny

echo "Disable blob public access"
az storage account update --resource-group $SA_RESOURCE_GROUP --name $STORAGE_ACCOUNT_NAME --allow-blob-public-access false

echo "Set min TLS version to 1.2"
az storage account update --resource-group $SA_RESOURCE_GROUP --name $STORAGE_ACCOUNT_NAME --min-tls-version TLS1_2

echo "Get AKS Network"
AKS_NETWORK=$(az network vnet list -g ${AKS_RESOURCE_GROUP} | grep providers/Microsoft.Network/virtualNetworks/)

echo "Add network rule to only allow the AKS vnets/subnets access the storage account"
echo $AKS_NETWORK | sed -z 's/,/\n/g'| grep "Microsoft.Network/virtualNetworks" | grep -v "/subnets/" | while read vnet_line
do
  #echo "$vnet_line"
  vnet_name=$(echo $vnet_line | sed 's/.*Microsoft.Network\/virtualNetworks\/\(.*\)\".*/\1/')
  #echo "vnet name: $vnet_name"
  subnet_name=$(echo $AKS_NETWORK | sed "s/.*Microsoft.Network\/virtualNetworks\/${vnet_name}\/subnets\/\(.*\)\".*/\1/")

  #echo "subnet name: $subnet_name"
  az network vnet subnet update --resource-group $AKS_RESOURCE_GROUP --vnet-name ${vnet_name}  --name ${subnet_name} --service-endpoints "Microsoft.Storage"

  #echo "subnetid: $subnet_id"
  subnet_id=$(az network vnet subnet show --resource-group $AKS_RESOURCE_GROUP --vnet-name ${vnet_name} --name ${subnet_name} --query id --output tsv)

  #Add network rule
  az storage account network-rule add -g $SA_RESOURCE_GROUP --account-name $STORAGE_ACCOUNT_NAME  --subnet ${subnet_id}
done
