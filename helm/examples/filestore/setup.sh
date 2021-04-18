#!/bin/bash

CUR_DIR=$(pwd)
WORK_DIR=$(dirname $0)


helm repo list | grep -q nfs-subdir-external-provisioner
[[ $? -ne 0 ]] && helm repo add nfs-subdir-external-provisioner https://kubernetes-sigs.github.io/nfs-subdir-external-provisioner/

cd ${WORK_DIR}/hpcc-filestore
helm dependency update
cd $CUR_DIR
