#!/bin/bash

## Normally you shouldn't need to run this script.
## Delete EKS cluster should automatically delete the "Open ID Connect Provider"
WORK_DIR=$(dirname $0)

source ${WORK_DIR}/efs-env

aws iam list-open-id-connect-providers

#oidc_id=$(aws eks describe-cluster --name ${EKS_NAME} --query "cluster.identity.oidc.issuer" --output text | cut -d '/' -f 5)
#echo "oidc_id: $oidc_id"
#oidc_providers=$(aws iam list-open-id-connect-providers | grep $oidc_id | awk '{print $2}')
#echo $oidc_providers

# aws iam delete-open-id-connect-provider --open-id-connect-provider-arn <open-id-connect-provider>
