#!/bin/bash
WORK_DIR=$(dirname $0)
source ${WORK_DIR}/env-sa
source ${WORK_DIR}/common-libs

if [[ -z "$SA_RESOURCE_GROUP" ]] || [[ -z "$SA_LOCATION" ]] || [[ -z "$STORAGE_ACCOUNT_NAME" ]]
then
   echo "Miss one of SA_RESOURCE_GROUP, SA_LOCATION and STORAGE_ACCOUNT_NAME"
   exit 1
fi

[[ -n "$SUBSCRIPTION" ]] && az account set --subscription $SUBSCRIPTION

VALUES_FILE=${WORK_DIR}/../hpcc-azurefile/values.yaml
SECRET_NAME_INPUT=$(cat $VALUES_FILE | grep "^[[:space:]]*secretName:"|cut -d':' -f2|sed 's/[[:space:]\"]//g')
SECRET_NAMESPACE_INPUT=$(cat $VALUES_FILE | grep "^[[:space:]]*secretNamespace:"|cut -d':' -f2|sed 's/[[:space:]\"]//g')
[[ -n "$SECRET_NAME_INPUT" ]] && SECRET_NAME=${SECRET_NAME_INPUT}
[[ -n "$SECRET_NAMESPACE_INPUT" ]] && SECRET_NAMESPACE=${SECRET_NAMESPACE_INPUT}
get_share_names
[[ -n "$SHARE_NAMES_INPUT" ]] && SHARE_NAMES=${SHARE_NAMES_INPUT}
SHARE_NAMES=$(echo $SHARE_NAMES | sed 's/,/ /g' | tr -s ' ' | sed 's/^ $//g'  )

if [[ -z "$SECRET_NAME" ]] || [[ -z "$SECRET_NAMESPACE" ]] || [[ -z "$SHARE_NAMES" ]]
then
   echo "Miss one of SECRET_NAME, SECRET_NAMESPACE and SHARE_NAMES"
   exit 1
fi

# Create a resource group
# Check Resource Group
rc=$(az group exists --name ${SA_RESOURCE_GROUP})
if [ "$rc" != "true" ]
then
  az group create --name ${SA_RESOURCE_GROUP} --location ${SA_LOCATION} --tags ${TAGS}
fi

az storage account check-name -n $STORAGE_ACCOUNT_NAME | \
	grep -q "\"reason\":[[:space:]]\"AlreadyExist\""
if [ $? -ne 0 ]
then
  # Create a storage account
  az storage account create \
    -n $STORAGE_ACCOUNT_NAME \
    -g $SA_RESOURCE_GROUP \
    -l $SA_LOCATION \
    --sku $SA_SKU \
    --tags ${TAGS}
fi
# Export the connection string as an environment variable,
# this is used when creating the Azure file share
export AZURE_STORAGE_CONNECTION_STRING=$(az storage account show-connection-string \
  -n $STORAGE_ACCOUNT_NAME -g $SA_RESOURCE_GROUP -o tsv)

for shareName in $SHARE_NAMES
do
  az storage share exists --connection-string "${AZURE_STORAGE_CONNECTION_STRING}" \
    --name  $shareName | grep -q  "\"exists\":[[:space:]]*false"
  if [ $? -eq 0 ]
  then
    echo "create share $shareName"
    az storage share create \
      -n $shareName \
      --connection-string "${AZURE_STORAGE_CONNECTION_STRING}"
  fi
done

# Get storage account key
STORAGE_KEY=$(az storage account keys list \
  --resource-group $SA_RESOURCE_GROUP \
  --account-name $STORAGE_ACCOUNT_NAME \
  --query "[0].value" -o tsv)

# Echo storage account name and key
# echo Storage account name: $STORAGE_ACCOUNT_NAME
mkdir -p $SA_KEY_DIR
echo Storage account key: $STORAGE_KEY > ${SA_KEY_DIR}/${STORAGE_ACCOUNT_NAME}.key
# cat ${SA_KEY_DIR}/${STORAGE_ACCOUNT_NAME}.key
