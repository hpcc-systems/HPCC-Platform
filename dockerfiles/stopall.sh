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
force=0
CLUSTERNAME=mycluster
UNINSTALL_ELK=1

while [ "$#" -gt 0 ]; do
  arg=$1
  case "${arg}" in
      -w) wait=1
         ;;
      -f) force=1
         ;;
      -n) shift
         CLUSTERNAME=$1
         ;;
      -e) UNINSTALL_ELK=0
	     echo "elastic4hpcclogs will not be stopped..."
         ;;
      *) echo "Usage: stoptall.sh [options]"
         echo "    -w  Wait for all pods to terminate"
		 echo "    -e  Suppress deletion of elastic4hpcclogs"
         exit
         ;;
    esac
  shift
done

helm uninstall $CLUSTERNAME
helm uninstall localfile
helm uninstall myprometheus4hpccmetrics
kubectl delete jobs --all
kubectl delete networkpolicy --all

if [[ $UNINSTALL_ELK == 1 ]] ; then
  echo "Uninstalling myelastic4hpcclogs:"
  helm uninstall myelastic4hpcclogs

  echo "Deleting Elasticsearch PVC..."
  kubectl delete pvc elasticsearch-master-elasticsearch-master-0
fi

if [[ $force == 1 ]] ; then
  sleep 1
  for f in `kubectl get pods | grep -q ^NAME | awk '{print $1}'` ; do
    kubectl delete pod $f --force --grace-period=0
  done
fi

if [[ $wait == 1 ]] ; then
  sleep 2
  while (kubectl get pods | grep -q ^NAME) ; do
    echo Waiting...
    sleep 2
  done
fi
