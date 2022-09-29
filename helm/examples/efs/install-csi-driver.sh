#!/bin/bash

WORK_DIR=$(dirname $0)

source ${WORK_DIR}/efs-env
echo "AWS_PROFILE:  $AWS_PROFILE"

roleName=${EKS_NAME}_EFS_CSI_Role

create_efs_csi_driver_policy()
{
  echo "create efs csi driver policy"
  #aws iam list-policies | grep -q AmazonEKS_EFS_CSI_Driver_Policy # [Errno 32] Broken pipe on WSL
  aws iam list-policies | awk '/AmazonEKS_EFS_CSI_Driver_Policy/{print $2}' | grep -q EFS
  [ $? -ne 0 ] && \
  aws iam create-policy \
    --policy-name AmazonEKS_EFS_CSI_Driver_Policy \
    --policy-document file://${WORK_DIR}/iam-policy-example.json
}

create_iam_role()
{
  echo "create efs csi driver iam role"
  # Delete role
  ${WORK_DIR}/delete-role.sh > /dev/null 2>&1

  #aws iam list-roles | grep -q AmazonEKS_EFS_CSI_DriverRole
  aws iam list-roles | awk '/${roleName}/{print $2}' | grep -q EFS
  if [ $? -ne 0 ]
  then
    OIDC_URL=$(aws eks describe-cluster --name ${EKS_NAME} --region ${EFS_REGION} --query "cluster.identity.oidc.issuer" --output text)
    OIDC_PROVIDER=${OIDC_URL##*/}
    sed "s/<ACCOUNT_ID>/${ACCOUNT_ID}/g; \
         s/<REGION_CODE>/${EFS_REGION}/g; \
	 s/<OIDC_PROVIDER>/${OIDC_PROVIDER}/g" ${WORK_DIR}/trust-policy.json.template > ${WORK_DIR}/trust-policy.json
    echo "aws iam create-role \
      --role-name ${roleName} \
      --assume-role-policy-document file://${WORK_DIR}/trust-policy.json"
    aws iam create-role \
      --role-name ${roleName} \
      --assume-role-policy-document file://"${WORK_DIR}/trust-policy.json"
    aws iam attach-role-policy \
       --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/AmazonEKS_EFS_CSI_Driver_Policy \
       --role-name ${roleName}
    #rm -rf  ${WORK_DIR}/trust-policy.json
  fi
}

create_efs_service_account()
{
    echo "create efs service account"
    sed "s/<ACCOUNT_ID>/${ACCOUNT_ID}/g; \
         s/<ROLE_NAME>/${roleName}/g"  ${WORK_DIR}/efs-service-account.yaml.template > ${WORK_DIR}/efs-service-account.yaml
    kubectl apply -f ${WORK_DIR}/efs-service-account.yaml
    #rm -rf ${WORK_DIR}/efs-service-account.yaml
}

install_amazon_efs_driver()
{
  echo "install then amazon efs driver"
  helm repo add aws-efs-csi-driver https://kubernetes-sigs.github.io/aws-efs-csi-driver/
  helm repo update
  helm upgrade -i aws-efs-csi-driver aws-efs-csi-driver/aws-efs-csi-driver \
    --namespace kube-system \
    --set image.repository=${EFS_CSI_DRIVER_ACCOUNT_ID_BY_REGION}.dkr.ecr.${EFS_REGION}.amazonaws.com/eks/aws-efs-csi-driver \
    --set controller.serviceAccount.create=false \
    --set controller.serviceAccount.name=efs-csi-controller-sa
}

create_storage_class_yaml()
{
    echo ""
    echo "create storageclass.yaml from storageclass.yaml.template"
    #echo "EFS_ID: ${EFS_ID}  EFS_BASE_PATH: $EFS_BASE_PATH"
    sed "s/<EFS_ID>/${EFS_ID}/g; \
	 s/<EFS_BASE_PATH>/\\${EFS_BASE_PATH}/g"  ${WORK_DIR}/storageclass.yaml.template > ${WORK_DIR}/storageclass.yaml
}

helm list | grep -q ${EFS_NAME}
if [[ $? -ne 0 ]]
then
  create_efs_csi_driver_policy
  create_iam_role
  create_efs_service_account
  ${WORK_DIR}/associate-oidc.sh
  install_amazon_efs_driver
  create_storage_class_yaml
else
  echo "efs-csi-driver $EFS_NAME may already exists."
fi
