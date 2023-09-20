#!/bin/bash
#reference: https://docs.aws.amazon.com/eks/latest/userguide/fsx-csi.html

WORK_DIR=$(dirname $0)

source ${WORK_DIR}/fsx-env
echo "AWS_PROFILE:  $AWS_PROFILE"

create_iam_role_and_kubernetes_service_account()
{
  echo "creating iam role and kubernetes service account"
  eksctl utils associate-iam-oidc-provider --region=${EKS_REGION} --cluster=${EKS_NAME} --approve
  eksctl create iamserviceaccount \
    --name fsx-csi-controller-sa \
    --namespace kube-system \
    --cluster ${EKS_NAME} \
    --attach-policy-arn arn:aws:iam::aws:policy/${POLICY_NAME} \
    --approve \
    --role-name ${ROLE_NAME} \
    --override-existing-serviceaccounts \
    --region ${EKS_REGION}
}

install_fsx_driver()
{
    echo ""
    echo "installing the fsx csi driver helm chart"
    helm repo add aws-fsx-csi-driver https://kubernetes-sigs.github.io/aws-fsx-csi-driver
    helm repo update
    helm upgrade \
    --install aws-fsx-csi-driver \
    --namespace kube-system \
    aws-fsx-csi-driver/aws-fsx-csi-driver \
    --set csidriver.fsGroupPolicy=File \
    --set controller.serviceAccount.create=false \
    --set controller.serviceAccount.name=fsx-csi-controller-sa
}

create_storage_class_yaml()
{
    echo ""
    echo "creating storageclass.yaml from storageclass.yaml.template"
    #echo "EFS_ID: ${EFS_ID}  EFS_BASE_PATH: $EFS_BASE_PATH"
    sed "s/<SUBNET_ID>/${SUBNET_ID}/g; \
     s/<SG_ID>/${SG_ID}/g" ${WORK_DIR}/storageclass.yaml.template > ${WORK_DIR}/storageclass.yaml
}

create_iam_role_and_kubernetes_service_account
install_fsx_driver
create_storage_class_yaml