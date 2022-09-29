#!/bin/bash
WORK_DIR=$(dirname $0)

source ${WORK_DIR}/efs-env

echo "Create a inbound rule for NFS in the security group"
cidr_range=$(aws ec2 describe-vpcs --vpc-ids ${VPC_ID} --query "Vpcs[].CidrBlock" --output text --region ${EFS_REGION})
echo "CIDR: $cidr_range"

echo "aws ec2 authorize-security-group-ingress \
    --group-id ${EFS_SECURITY_GROUPS} \
    --protocol tcp \
    --port 2049 \
    --region ${EFS_REGION} \
    --cidr $cidr_range"

aws ec2 authorize-security-group-ingress \
    --group-id ${EFS_SECURITY_GROUPS} \
    --protocol tcp \
    --port 2049 \
    --region ${EFS_REGION} \
    --cidr $cidr_range
