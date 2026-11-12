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
set -e

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
      -h|--help)
         echo "Usage: stopall.sh [options]"
         echo "    -w  Wait for all pods to terminate"
         echo "    -f  Force deletion of remaining pods"
         echo "    -n <name>  Specify cluster name (default: mycluster)"
         echo "    -e  Suppress deletion of elastic4hpcclogs"
         echo "Uses Helm and kubectl; no local Docker or Podman engine is required."
         exit
         ;;
      *) echo "Unknown option: ${arg}. Use -h for help." >&2
         exit 1
         ;;
    esac
  shift
done

helm uninstall "$CLUSTERNAME" --ignore-not-found
helm uninstall localfile --ignore-not-found
helm uninstall myprometheus4hpccmetrics --ignore-not-found
helm uninstall myloki4hpcclogs --ignore-not-found
kubectl delete jobs --all --ignore-not-found
kubectl delete networkpolicy --all --ignore-not-found

if [[ $UNINSTALL_ELK == 1 ]] ; then
  echo "Uninstalling myelastic4hpcclogs:"
  helm uninstall myelastic4hpcclogs --ignore-not-found

  echo "Deleting Elasticsearch PVC..."
  kubectl delete pvc elasticsearch-master-elasticsearch-master-0 --ignore-not-found
fi

if [[ $force == 1 ]] ; then
  sleep 1
  kubectl delete pods --all --force --grace-period=0 --ignore-not-found
fi

if [[ $wait == 1 ]] ; then
  sleep 2
  while true ; do
    pods=$(kubectl get pods --output=name)
    [[ -n "$pods" ]] || break
    echo Waiting...
    sleep 2
  done
fi
