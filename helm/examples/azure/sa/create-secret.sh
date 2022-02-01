#!/bin/bash
WORK_DIR=$(dirname $0)
source ${WORK_DIR}/env-sa

[[ -n "$SUBSCRIPTION" ]] && az account set --subscription $SUBSCRIPTION

VALUES_FILE=${WORK_DIR}/../hpcc-azurefile/values.yaml
SECRET_NAME_INPUT=$(cat $VALUES_FILE | grep "^[[:space:]]*secretName:"|cut -d':' -f2|sed 's/[[:space:]\"]//g')
SECRET_NAMESPACE_INPUT=$(cat $VALUES_FILE | grep "^[[:space:]]*secretNamespace:"|cut -d':' -f2|sed 's/[[:space:]\"]//g')
[[ -n "$SECRET_NAME_INPUT" ]] && SECRET_NAME=${SECRET_NAME_INPUT}
[[ -n "$SECRET_NAMESPACE_INPUT" ]] && SECRET_NAMESPACE=${SECRET_NAMESPACE_INPUT}


if [[ -z "$SECRET_NAME" ]] || [[ -z "$SECRET_NAMESPACE" ]]
then
  echo "Miss one of SECRET_NAME and SECRET_NAMESPACE"
  exit 1
fi

if [[ ! -e "$SA_KEY_DIR/${STORAGE_ACCOUNT_NAME}.key" ]]
then
  echo "Cannot find Azure storage account key file: $SA_KEY_DIR/${STORAGE_ACCOUNT_NAME}.key"
  exit 1
fi

kubectl get secret -n $SECRET_NAMESPACE  | cut -d' ' -f1 | grep -q "$SECRET_NAME"
if [[ $? -ne 0 ]]
then
  echo "create secret $SECRET_NAME"
  account_key=$(cat ${SA_KEY_DIR}/${STORAGE_ACCOUNT_NAME}.key | cut -d':' -f2 | sed 's/[[:space:]]*//g')
  kubectl create secret generic $SECRET_NAME -n $SECRET_NAMESPACE \
    --from-literal="azurestorageaccountname=${STORAGE_ACCOUNT_NAME}" \
    --from-literal="azurestorageaccountkey=${account_key}"
else
  echo "Secret $SECRET_NAME already exists"
fi
