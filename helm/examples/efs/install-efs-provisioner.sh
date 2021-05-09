#!/bin/bash

WORK_DIR=$(dirname $0)

source ${WORK_DIR}/efs-env

install_efs_provisioner()
{
  # Add efs-provistioner Helm Chart
  helm repo add efs-provisioner https://charts.helm.sh/stable

  # Get EKS security group
  which aws > /dev/null
  if [[ $? -eq 0 ]] && [[ -n "$EKS_NAME" ]] && [[ -n "$EFS_SECURITY_GROUPS" ]]
  then
    echo "aws eks describe-cluster "
    echo "  --name $EKS_NAME"
    echo "  --region $EFS_REGION"
    echo "  --query cluster.resourcesVpcConfig.clusterSecurityGroupId"
    eks_security_group_id=$(aws eks describe-cluster \
      --name $EKS_NAME \
      --region $EFS_REGION \
      --query cluster.resourcesVpcConfig.clusterSecurityGroupId)
    if [[ -z $eks_security_group_id ]]
    then
      echo "Cannot find EKS scurity group id with AWS profile $AWS_PROFILE,"
      echo "EKS name $EKS_NAME in region $EFS_REGSION"
      exit 1
    fi
    echo "EKS security gorup id: $eks_security_group_id"

    # Authorize inbound access to the EKS security groups for EFS mount targets
    for group_id in ${EFS_SECURITY_GROUPS}
    do
      echo "aws ec2 authorize-security-group-ingress"
      echo "  --group-id $group_id"
      echo "  --protocol tcp"
      echo "  --port 2049"
      echo "  --source-group $eks_security_group_id"
      echo "  --region $AWS_REGION"
      aws ec2 authorize-security-group-ingress \
        --group-id $group_id \
        --protocol tcp \
        --port 2049 \
        --source-group $eks_security_group_id --region $EFS_REGION
    done
  fi

  # create efs-provisiner
  if [[ "${EFS_CSI_DRIVER}" == "true" ]]
  then
    echo "kubectl apply -k \"github.com/kubernetes-sigs/aws-efs-csi-driver/deploy/kubernetes/overlays/stable/ecr/?ref=release-1.0\""
    kubectl apply -k "github.com/kubernetes-sigs/aws-efs-csi-driver/deploy/kubernetes/overlays/stable/ecr/?ref=release-1.0"
  fi

  helm install ${EFS_NAME} \
    efs-provisioner/efs-provisioner \
    --set efsProvisioner.efsFileSystemId=${EFS_ID} \
    --set efsProvisioner.awsRegion=${EFS_REGION} \
    --set efsProvisioner.storageClass.reclaimPolicy=${RECLAIM_POLICY}
}

helm list | grep -q ${EFS_NAME}
if [[ $? -ne 0 ]]
then
  install_efs_provisioner
else
  echo "efs-provisioner $EFS_NAME may already exists."
fi
