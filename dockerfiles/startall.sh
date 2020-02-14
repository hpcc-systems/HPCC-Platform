#!/bin/bash

HEAD=$(git rev-parse --short HEAD)

kubectl run dali --image=hpccsystems/dali:${HEAD} --image-pull-policy=Never
kubectl expose deployment dali --port=7070
kubectl run esp --image=hpccsystems/esp:${HEAD} --image-pull-policy=Never
kubectl expose deployment esp --port=8010 --type=LoadBalancer
kubectl run roxie --image=hpccsystems/roxie:${HEAD} --image-pull-policy=Never
kubectl run eclcc --image=hpccsystems/eclccserver:${HEAD} --image-pull-policy=Never
kubectl run eclagent --image=hpccsystems/eclagent:${HEAD} --image-pull-policy=Never
kubectl run thormaster --image=hpccsystems/thormaster:${HEAD} --image-pull-policy=Never
kubectl expose deployment thormaster --port=20000
kubectl run thorslave --image=hpccsystems/thorslave:${HEAD} --image-pull-policy=Never master=thormaster

#kubectl logs mydali-759f975769-v6tmm

