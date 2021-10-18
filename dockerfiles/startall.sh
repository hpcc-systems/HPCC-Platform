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

# Utility script for starting a local cluster corresponding to current git branch
# Usage startup.sh [<image.version>] [<helm-install-options>]

DOCKER_REPO=hpccsystems
scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
restArgs=()
CLUSTERNAME=mycluster
PVFILE=$scriptdir/../helm/examples/local/hpcc-localfile/values.yaml

dependency_check () {

  if [ -z "$1" ]
  then
      CHART_SUBPATH="hpcc"
  else
      CHART_SUBPATH=$1
  fi

  missingDeps=0
  while IFS= read -r line
  do
    echo "${line}"
    if echo "${line}" | egrep -q 'missing$'; then
      let "missingDeps++"
    fi
  done < <(helm dependency list ${scriptdir}/../helm/${CHART_SUBPATH} | grep -v WARNING)
  if [[ ${missingDeps} -gt 0 ]]; then
    echo "Some of the chart dependencies are missing."
    echo "Either issue a 'helm dependency update ${scriptdir}/../helm/${CHART_SUBPATH}' to fetch them,"
    echo "or rerun $0 with option -c to auto update them."
    exit 0
  fi
}

CMD="install"
DEVELOPER_OPTIONS="--set global.privileged=true"
while [ "$#" -gt 0 ]; do
  arg=$1
  if [[ ${arg:0:1} == '-' ]]; then
    case "${arg:1:1}" in
      l) shift
         LABEL=$1
         ;;
      n) shift
         CLUSTERNAME=$1
         ;;
      d) shift;
         INPUT_DOCKER_REPO=$1
         ;;
      u) CMD="upgrade"
         ;;
      p) shift
         if [[ $arg == '-pv' ]] ; then
           PERSISTVALUES="--values=$1"
           PVFILE=$1
         else
           PERSIST=$1
         fi
         ;;
      c) DEP_UPDATE_ARG="--dependency-update"
         ;;
      h) echo "Usage: startall.sh [options]"
         echo "    -d <docker-repo>   Docker repository to fetch images from"
         echo "    -l                 Build image label to use"
         echo "    -u                 Use "upgrade" rather than "install""
         echo "    -t                 Generate templates instead of starting the system"
         echo "    -n <name>          Specify cluster name"
         echo "    -c                 Update chart dependencies"
         echo "    -p <location>      Use local persistent data"
         echo "    -pv <yamlfile>     Override dataplane definitions for local persistent data"
         echo "    -e                 Deploy light-weight Elastic Stack for component log processing"
         echo "    -m                 Deploy Prometheus Stack for component metrics processing"
         exit
         ;;
      t) CMD="template"
         restArgs+="--debug"
         ;;
      # vanilla install - for testing system in the same way it will normally be used
      v) DEVELOPER_OPTIONS=""
         ;;
      e) DEPLOY_ES=1
         ;;
      m) DEPLOY_PROM=1
         PROMETHEUS_METRICS_SINK_ARG="-f $scriptdir/../helm/examples/metrics/prometheus_metrics.yaml"
         ;;
      *) restArgs+=(${arg})
         ;;
    esac
  else
    restArgs+=(${arg})
  fi
  shift
done


if [[ -n "${DEP_UPDATE_ARG}" ]]; then
  if [[ "${CMD}" = "upgrade" ]]; then
    echo "Chart dependencies cannot be updated whilst performing a helm upgrade"
    DEP_UPDATE_ARG=""
  fi
else
  dependency_check "hpcc"
fi

[[ -n ${INPUT_DOCKER_REPO} ]] && DOCKER_REPO=${INPUT_DOCKER_REPO}
[[ -z ${LABEL} ]] && LABEL=$(docker image ls | fgrep "${DOCKER_REPO}/platform-core" | head -n 1 | awk '{print $2}')

if [[ -n ${PERSIST} ]] ; then
  PERSIST=$(realpath -q $PERSIST || echo $PERSIST)
  PERSIST_PATH=$(echo $PERSIST | sed 's/\\//g')
  for subdir in `grep subPath: $PVFILE | awk '{ print $2 }'` ; do
    echo mkdir -p ${PERSIST_PATH}/$subdir
    mkdir -p ${PERSIST_PATH}/$subdir
  done
  helm ${CMD} localfile $scriptdir/../helm/examples/local/hpcc-localfile --set common.hostpath=${PERSIST} $PERSISTVALUES | tee lsfull.yaml | grep -A1000 storage: > localstorage.yaml && \
  grep "##" lsfull.yaml  && \
  helm ${CMD} $CLUSTERNAME $scriptdir/../helm/hpcc/ --set global.image.root="${DOCKER_REPO}" --set global.image.version=$LABEL $DEVELOPER_OPTIONS $DEP_UPDATE_ARG ${restArgs[@]} -f localstorage.yaml ${PROMETHEUS_METRICS_SINK_ARG}
else
  helm ${CMD} $CLUSTERNAME $scriptdir/../helm/hpcc/ --set global.image.root="${DOCKER_REPO}" --set global.image.version=$LABEL $DEVELOPER_OPTIONS $DEP_UPDATE_ARG ${restArgs[@]} ${PROMETHEUS_METRICS_SINK_ARG}
fi

if [[ $DEPLOY_ES == 1 ]] ; then
  echo -e "\n\nDeploying "myelastic4hpcclogs" - light-weight Elastic Stack:"
  if [[ -z "${DEP_UPDATE_ARG}" ]]; then
    dependency_check "managed/logging/elastic"
  fi
  helm ${CMD} myelastic4hpcclogs $scriptdir/../helm/managed/logging/elastic $DEP_UPDATE_ARG ${restArgs[@]}
fi

if [[ $DEPLOY_PROM == 1 ]] ; then
  echo -e "\n\nDeploying "myprometheus4hpccmetrics" - Prometheus Stack:"
  if [[ -z "${DEP_UPDATE_ARG}" ]]; then
    dependency_check "managed/metrics/prometheus"
  fi
  helm ${CMD} myprometheus4hpccmetrics $scriptdir/../helm/managed/metrics/prometheus $DEP_UPDATE_ARG ${restArgs[@]} --set kube-prometheus-stack.prometheus.service.type=LoadBalancer --set kube-prometheus-stack.grafana.service.type=LoadBalancer
fi

if [ ${CMD} != "template" ] ; then
  sleep 1
  kubectl get pods
fi
