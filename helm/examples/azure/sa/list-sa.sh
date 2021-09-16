#!/bin/bash
WORK_DIR=$(dirname $0)
source ${WORK_DIR}/env-sa

VALUES_FILE=${WORK_DIR}/../hpcc-azurefile/values.yaml

# list share
account_key=$(cat ${SA_KEY_DIR}/${STORAGE_ACCOUNT_NAME}.key | cut -d':' -f2 | sed 's/[[:space:]]\+//g')
az storage share list --account-name ${STORAGE_ACCOUNT_NAME} --account-key "$account_key"
