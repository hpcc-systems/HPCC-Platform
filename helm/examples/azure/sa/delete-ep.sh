#!/bin/bash
WORK_DIR=$(dirname $0)
source ${WORK_DIR}/env-sa


# Set subscription
[[ -n "$SUBSCRIPTION" ]] && az account set --subscription $SUBSCRIPTION

# Delete each endpoint

rc=$(az group exists --name ${SA_RESOURCE_GROUP})
if [ "$rc" = "true" ]
then
  az network private-endpoint list -g ${SA_RESOURCE_GROUP} --query '[].[name]' --output tsv | \
  while read pep
  do
     [ -z "$pep" ] && continue
     echo "az network private-endpoint delete --name ${pep} -g ${SA_RESOURCE_GROUP}"
     az network private-endpoint delete --name ${pep} -g ${SA_RESOURCE_GROUP}
  done
fi
