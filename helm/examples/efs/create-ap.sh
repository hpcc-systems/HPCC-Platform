#!/bin/bash
WORK_DIR=$(dirname $0)

source ${WORK_DIR}/efs-env

echo "creating access points for ${EFS_ID}"
aws efs create-access-point \
    --file-system-id ${EFS_ID} \
    --posix-user Gid=10001,Uid=10000 \
    --root-directory Path='/hpcc/dali,CreationInfo={OwnerUid=10000,OwnerGid=10001,Permissions=0700}' \
    --tags Key=Name,Value=dali-ap
aws efs create-access-point \
    --file-system-id ${EFS_ID} \
    --posix-user Gid=10001,Uid=10000 \
    --root-directory Path='/hpcc/dll,CreationInfo={OwnerUid=10000,OwnerGid=10001,Permissions=0700}' \
    --tags Key=Name,Value=dll-ap
aws efs create-access-point \
    --file-system-id ${EFS_ID} \
    --posix-user Gid=10001,Uid=10000 \
    --root-directory Path='/hpcc/sasha,CreationInfo={OwnerUid=10000,OwnerGid=10001,Permissions=0700}' \
    --tags Key=Name,Value=sasha-ap
aws efs create-access-point \
    --file-system-id ${EFS_ID} \
    --posix-user Gid=10001,Uid=10000 \
    --root-directory Path='/hpcc/data,CreationInfo={OwnerUid=10000,OwnerGid=10001,Permissions=0700}' \
    --tags Key=Name,Value=data-ap
aws efs create-access-point \
    --file-system-id ${EFS_ID} \
    --posix-user Gid=10001,Uid=10000 \
    --root-directory Path='/hpcc/mydropzone,CreationInfo={OwnerUid=10000,OwnerGid=10001,Permissions=0700}' \
    --tags Key=Name,Value=mydropzone-ap