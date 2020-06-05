#!/bin/bash
WK_DIR=$(dirname $0)
NAMESPACE=$(grep "^[[:space:]]*namespace" ${WK_DIR}/values.yaml | cut -d':' -f2 | sed -e 's/[[:space:]]//g')

kubectl -n ${NAMESPACE} get pv | grep "dali-efsstorage-hpcc-efs-pvc\|dll-efsstorage-hpcc-efs-pvc\|data-efsstorage-hpcc-efs-pvc" | \
while read name others
do
 #echo "kubectl delete pv $name"
 kubectl delete pv $name
done
