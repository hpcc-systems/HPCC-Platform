#!/bin/bash

kubectl delete deployments.apps dali roxie esp eclcc eclagent thormaster thorslave
kubectl delete service dali
kubectl delete service esp
kubectl delete service thormaster
