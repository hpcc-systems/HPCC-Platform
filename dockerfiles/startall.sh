#!/bin/bash

BUILD_VER=$1
[[ -z ${BUILD_VER} ]] && BUILD_VER=${INPUT_BUILD_VER}
[[ -z ${BUILD_VER} ]] && BUILD_VER=$(git describe --exact-match --tags)
[[ -z ${BUILD_VER} ]] && BUILD_VER=$(git rev-parse --short ${BUILD_VER})

kubectl run dali --image=hpccsystems/dali:${BUILD_VER} --image-pull-policy=Never
kubectl expose deployment dali --port=7070
kubectl run esp --image=hpccsystems/esp:${BUILD_VER} --image-pull-policy=Never
kubectl expose deployment esp --port=8010 --type=LoadBalancer
kubectl run roxie --image=hpccsystems/roxie:${BUILD_VER} --image-pull-policy=Never
kubectl run eclcc --image=hpccsystems/eclccserver:${BUILD_VER} --image-pull-policy=Never
kubectl run eclagent --image=hpccsystems/eclagent:${BUILD_VER} --image-pull-policy=Never
kubectl run thormaster --image=hpccsystems/thormaster:${BUILD_VER} --image-pull-policy=Never
kubectl expose deployment thormaster --port=20000
kubectl run thorslave --image=hpccsystems/thorslave:${BUILD_VER} --image-pull-policy=Never master=thormaster

#kubectl logs mydali-759f975769-v6tmm

