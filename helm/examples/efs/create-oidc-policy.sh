#!/bin/bash
WORK_DIR=$(dirname $0)

source ${WORK_DIR}/efs-env

echo "create OIDC-Policy"
sed "s/<ACCOUNT_ID>/${ACCOUNT_ID}/g"  ${WORK_DIR}/OIDC-Policy.json.template > ${WORK_DIR}/OIDC-Policy.json
aws iam create-policy \
    --policy-name OIDC \
    --policy-document file://OIDC-Policy.json
