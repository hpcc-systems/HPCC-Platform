#!/bin/bash

HEAD=$(git rev-parse --short HEAD)

helm install mycluster hpcc/ --set global.image.version=$HEAD 
sleep 1
kubectl get pods

