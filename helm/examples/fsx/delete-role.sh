#!/bin/bash

WORK_DIR=$(dirname $0)

source ${WORK_DIR}/fsx-env
echo "AWS_PROFILE:  $AWS_PROFILE"

aws iam detach-role-policy --role-name ${ROLE_NAME} > /dev/null 2>&1
if [ $? -eq 0 ]
then
  aws iam detach-role-policy --role-name ${ROLE_NAME} --policy-arn arn:aws:iam::aws:policy/${POLICY_NAME}
  aws iam delete-role --role-name ${ROLE_NAME}
fi