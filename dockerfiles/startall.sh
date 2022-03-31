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

# Utility script for starting a local cluster (including elastic4hpcclogs) corresponding to current git branch
# Usage startup.sh [<image.version>] [<helm-install-options>]

DOCKER_REPO=hpccsystems
scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
restArgs=()
CLUSTERNAME=mycluster
PVFILE=$scriptdir/../helm/examples/local/hpcc-localfile/values.yaml
MKDIR=1

MANAGED_ELK_SUBPATH="managed/logging/elastic"
MANAGED_PROM_SUBPATH="managed/metrics/prometheus"

ELASTIC_LOG_ACCESS_ARG="-f $scriptdir/../helm/${MANAGED_ELK_SUBPATH}/elastic4hpcclogs-hpcc-logaccess.yaml"

dependency_check () {

  if [ -z "$1" ]
  then
      echo "dependency_check requires target chart subdir"
      exit 0
  else
      CHART_SUBPATH=$1
  fi

  missingDeps=0
  while IFS= read -r line
  do
    echo "${line}"
    if echo "${line}" | egrep -q 'missing$|wrong version$' ; then
      let "missingDeps++"
    fi
  done < <(helm dependency list ${scriptdir}/../helm/${CHART_SUBPATH} | grep -v WARNING)
  if [[ ${missingDeps} -gt 0 ]]; then
    echo "Some of the chart dependencies are missing or outdated."
    echo "Either issue a 'helm dependency update ${scriptdir}/../helm/${CHART_SUBPATH}' to fetch them,"
    echo "or rerun $0 with option -c to auto update them."
    exit 0
  fi
}

dependency_update () {

  if [ -z "$1" ]
  then
      echo "dependency_update requires target chart subdir"
      exit 0
  else
      CHART_SUBPATH=$1
  fi

  helm dependency update ${scriptdir}/../helm/${CHART_SUBPATH}
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
      p) if [[ $arg == '-pm' ]] ; then
           #Option for minikube to avoid trying to create the directories (because they are mounted elsewhere)
           MKDIR=0
         else
           shift
           if [[ $arg == '-pv' ]] ; then
             PERSISTVALUES="--values=$1"
             echo $PERSISTVALUES
             PVFILE=$1
           else
             PERSIST=$1
           fi
         fi
         ;;
      c) DEP_UPDATE=1
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
         echo "    -e                 Suppress deployment of elastic4hpcclogs (deployed by default)"
         echo "    -m                 Deploy Prometheus Stack for component metrics processing"
         exit
         ;;
      t) CMD="template"
         restArgs+="--debug"
         ;;
      # vanilla install - for testing system in the same way it will normally be used
      v) DEVELOPER_OPTIONS=""
         ;;
      e) ELASTIC_LOG_ACCESS_ARG=""
	       echo -e "\nDeployment of elastic4hpcclogs suppressed.\n"
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

if [[ "${CMD}" = "upgrade" ]]; then
  if [[ $(helm list -q -f $CLUSTERNAME) != $CLUSTERNAME ]]; then
    echo "Requested installation for upgrade does not exist - assuming install"
    CMD="install"
  fi
fi
[[ -n ${INPUT_DOCKER_REPO} ]] && DOCKER_REPO=${INPUT_DOCKER_REPO}
[[ -z ${LABEL} ]] && LABEL=$(docker image ls | fgrep "${DOCKER_REPO}/platform-core" | head -n 1 | awk '{print $2}')

if [[ -n ${PERSIST} ]] ; then
  PERSIST=$(realpath -q $PERSIST || echo $PERSIST)
  PERSIST_PATH=$(echo $PERSIST | sed 's/\\//g')
  if [[ $MKDIR == '1' ]] ; then
    for subdir in `grep subPath: $PVFILE | awk '{ print $2 }'` ; do
      echo mkdir -p ${PERSIST_PATH}/$subdir
      mkdir -p ${PERSIST_PATH}/$subdir
    done
  fi
  helm ${CMD} localfile $scriptdir/../helm/examples/local/hpcc-localfile --set common.hostpath=${PERSIST} $PERSISTVALUES | tee lsfull.yaml | grep -A1000 storage: > localstorage.yaml && \
  grep "##" lsfull.yaml  && \
  helm ${CMD} $CLUSTERNAME $scriptdir/../helm/hpcc/ --set global.image.root="${DOCKER_REPO}" --set global.image.version=$LABEL $DEVELOPER_OPTIONS ${restArgs[@]} -f localstorage.yaml ${PROMETHEUS_METRICS_SINK_ARG} ${ELASTIC_LOG_ACCESS_ARG}
else
  helm ${CMD} $CLUSTERNAME $scriptdir/../helm/hpcc/ --set global.image.root="${DOCKER_REPO}" --set global.image.version=$LABEL $DEVELOPER_OPTIONS ${restArgs[@]} ${PROMETHEUS_METRICS_SINK_ARG} ${ELASTIC_LOG_ACCESS_ARG}
fi

if [[ -n $ELASTIC_LOG_ACCESS_ARG ]] ; then
  echo -e "\n\nDeploying "myelastic4hpcclogs" - light-weight Elastic Stack:"
  if [[ $DEP_UPDATE == 1 ]]; then
    dependency_update $MANAGED_ELK_SUBPATH
  else
    dependency_check $MANAGED_ELK_SUBPATH
  fi
  helm ${CMD} myelastic4hpcclogs $scriptdir/../helm/$MANAGED_ELK_SUBPATH ${restArgs[@]}
fi

if [[ $DEPLOY_PROM == 1 ]] ; then
  echo -e "\n\nDeploying "myprometheus4hpccmetrics" - Prometheus Stack:"
  if [[ $DEP_UPDATE == 1 ]]; then
    dependency_update $MANAGED_PROM_SUBPATH
  else
    dependency_check $MANAGED_PROM_SUBPATH
  fi
  helm ${CMD} myprometheus4hpccmetrics $scriptdir/../helm/$MANAGED_PROM_SUBPATH ${restArgs[@]} --set kube-prometheus-stack.prometheus.service.type=LoadBalancer --set kube-prometheus-stack.grafana.service.type=LoadBalancer
fi

if [ ${CMD} != "template" ] ; then
  sleep 1
  kubectl get pods
fi
