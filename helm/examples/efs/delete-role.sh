#!/bin/bash
WORK_DIR=$(dirname $0)

source ${WORK_DIR}/efs-env

echo "deleting iam role"
echo "make sure you also uninstall the aws-efs-csi-driver helm chart"
STACK_NAME=eksctl-${EKS_NAME}-addon-iamserviceaccount-kube-system-efs-csi-controller-sa
aws cloudformation delete-stack --stack-name ${STACK_NAME}