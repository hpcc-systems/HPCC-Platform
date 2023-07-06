#!/bin/bash
#reference: https://docs.aws.amazon.com/eks/latest/userguide/efs-csi.html

WORK_DIR=$(dirname $0)

source ${WORK_DIR}/efs-env
echo "AWS_PROFILE:  $AWS_PROFILE"

roleName=${EKS_NAME}_EFS_CSI_Role
EFS_CSI_POLICY_NAME=EKS_EFS_CSI_Driver_Policy

create_efs_csi_driver_policy()
{
  echo "creating efs csi driver policy"
  #aws iam list-policies | grep -q $EFS_CSI_POLICY_NAME # [Errno 32] Broken pipe on WSL
  aws iam list-policies | awk "/${EFS_CSI_POLICY_NAME}/{print $2}" | grep -q EFS
  [ $? -ne 0 ] && \
  aws iam create-policy \
    --policy-name ${EFS_CSI_POLICY_NAME} \
    --policy-document file://${WORK_DIR}/iam-policy-example.json
}

create_iam_role_and_kubernetes_service_account()
{
  echo "creating iam role and kubernetes service account"
  eksctl utils associate-iam-oidc-provider --region=${EFS_REGION} --cluster=${EKS_NAME} --approve
  eksctl create iamserviceaccount \
    --cluster ${EKS_NAME} \
    --namespace kube-system \
    --name efs-csi-controller-sa \
    --attach-policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/${EFS_CSI_POLICY_NAME} \
    --approve \
    --override-existing-serviceaccounts \
    --region ${EFS_REGION}
}

install_amazon_efs_driver()
{
  echo "installing the amazon efs driver helm chart"
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
    echo "creating storageclass.yaml from storageclass.yaml.template"
    #echo "EFS_ID: ${EFS_ID}  EFS_BASE_PATH: $EFS_BASE_PATH"
    sed "s/<EFS_ID>/${EFS_ID}/g; \
	 s/<EFS_BASE_PATH>/\\${EFS_BASE_PATH}/g"  ${WORK_DIR}/storageclass.yaml.template > ${WORK_DIR}/storageclass.yaml
}

helm list | grep -q ${EFS_NAME}
if [[ $? -ne 0 ]]
then
  create_efs_csi_driver_policy
  create_iam_role_and_kubernetes_service_account
  ${WORK_DIR}/associate-oidc.sh
  install_amazon_efs_driver
  create_storage_class_yaml
else
  echo "efs-csi-driver $EFS_NAME may already exists."
fi
