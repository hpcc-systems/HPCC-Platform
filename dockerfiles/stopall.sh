#!/bin/bash

##############################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################

# Utility script for stopping a local cluster started by startall.sh
wait=0
CLUSTERNAME=mycluster

while [ "$#" -gt 0 ]; do
  arg=$1
  case "${arg}" in
      -w) wait=1
         ;;
      -n) shift
         CLUSTERNAME=$1
         ;;
      -e) UNINSTALL_ELK=1
         ;;
      *) echo "Usage: stoptall.sh [options]"
         echo "    -w  Wait for all pods to terminate"
         echo "    -e  Uninstall light-weight Elastic Stack"
         exit
         ;;
    esac
  shift
done

helm uninstall $CLUSTERNAME
helm uninstall localfile
kubectl delete jobs --all
kubectl delete networkpolicy --all
if [[ $UNINSTALL_ELK == 1 ]] ; then
  echo "Uninstalling myelastic4hpcclogs:"
  echo "PLEASE NOTE: Elastic Search declares PVC(s) which might require explicit manual removal if no longer needed."
  helm uninstall myelastic4hpcclogs
  kubectl get pvc
fi
if [[ $wait == 1 ]] ; then
  sleep 2
  while (kubectl get pods | grep -q ^NAME) ; do
    echo Waiting...
    sleep 2
  done
fi
