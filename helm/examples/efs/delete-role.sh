#!/bin/bash
WORK_DIR=$(dirname $0)

source ${WORK_DIR}/efs-env

aws iam detach-role-policy --role-name ${EKS_NAME}_EFS_CSI_Role \
  --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/AmazonEKS_EFS_CSI_Driver_Policy
aws iam delete-role --role-name ${EKS_NAME}_EFS_CSI_Role
