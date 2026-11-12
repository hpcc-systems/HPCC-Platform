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
KIND_OPTIONS=()
HELM_CONTEXT_OPTIONS=()
CLUSTERNAME=mycluster
PVFILE=$scriptdir/../helm/examples/local/hpcc-localfile/values.yaml
MKDIR=1

MANAGED_ELK_SUBPATH="managed/logging/elastic"
MANAGED_PROM_SUBPATH="managed/metrics/prometheus"
MANAGED_LOKI_SUBPATH="managed/logging/loki-stack"

LOG_FORMAT_ARG="table" # table/xml/json

ELASTIC_LOG_ACCESS_ARG="-f $scriptdir/../helm/${MANAGED_ELK_SUBPATH}/elastic4hpcclogs-hpcc-logaccess.yaml"

dependency_check () {

  if [ -z "$1" ]
  then
      echo "dependency_check requires target chart subdir"
      exit 1
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
    exit 1
  fi
}

dependency_update () {

  if [ -z "$1" ]
  then
      echo "dependency_update requires target chart subdir"
      exit 1
  else
      CHART_SUBPATH=$1
  fi

  helm dependency update ${scriptdir}/../helm/${CHART_SUBPATH} || exit 1
}

CMD="install"
DEVELOPER_OPTIONS="--set global.privileged=true --set global.omitResources=true"
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
         echo "    -d <image-repo>    Image repository to fetch images from"
         echo "    -l <tag>           Build image label to use (default: discover locally)"
         echo "    -u                 Use "upgrade" rather than "install""
         echo "    -t                 Generate templates instead of starting the system"
         echo "    -n <name>          Specify cluster name"
         echo "    -c                 Update chart dependencies"
         echo "    -p <location>      Use local persistent data"
         echo "    -pv <yamlfile>     Override dataplane definitions for local persistent data"
         echo "    -e                 Suppress deployment of elastic4hpcclogs (deployed by default)"
         echo "    -m                 Deploy Prometheus Stack for component metrics processing"
         echo "    -g                 Deploy Grafana Loki Stack for component logs processing"
         echo "Set CONTAINER_RUNTIME=docker or podman to select the engine for local tag discovery."
         echo "Supplying -l <tag> does not require a local container engine."
         exit
         ;;
      t) CMD="template"
         restArgs+=("--debug")
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
      g) DEPLOY_LOKI=1
         LOG_FORMAT_ARG="json"
         ;;
      *) restArgs+=("${arg}")
         ;;
    esac
  else
    restArgs+=("${arg}")
  fi
  shift
done

[[ -n ${INPUT_DOCKER_REPO} ]] && DOCKER_REPO=${INPUT_DOCKER_REPO}
# Validate repository input before passing it to the container image filter.
if ! [[ ${DOCKER_REPO} =~ ^[A-Za-z0-9._-]+(:[0-9]+)?(/[A-Za-z0-9._-]+)*$ ]]; then
  echo "ERROR: Image repository '${DOCKER_REPO}' contains invalid characters." >&2
  echo "       Allowed characters: letters, numbers, '.', '-', '_', optional ':<port>', and '/'." >&2
  exit 1
fi
if [[ -z ${LABEL} ]]; then
  source "${scriptdir}/chainguard/container-engine.sh" || exit 1
  case "${CONTAINER_RUNTIME}" in
    docker|podman) ;;
    *) echo "ERROR: Local image tag discovery requires Docker or Podman; selected runtime: ${CONTAINER_RUNTIME}. Supply -l <tag> instead." >&2; exit 1 ;;
  esac
  if ! IMAGE_TAGS=$(container_image_tags "${DOCKER_REPO}/platform-core"); then
    echo "ERROR: Could not list local image tags using ${CONTAINER_RUNTIME}." >&2
    exit 1
  fi
  LABEL=$(printf '%s\n' "${IMAGE_TAGS}" | awk 'NF && $0 != "<none>" { print; exit }')
  LOCAL_IMAGE=1
fi
if [[ -z ${LABEL} ]]; then
  echo "ERROR: No local image tag found for ${DOCKER_REPO}/platform-core. Build it with ${scriptdir}/image.sh or supply -l <tag>." >&2
  exit 1
fi

KUBE_CONTEXT="${HELM_KUBECONTEXT:-}"
KUBECONFIG_FILE=""
for ((i = 0; i < ${#restArgs[@]}; i++)); do
  case "${restArgs[i]}" in
    --kube-context|--kubeconfig)
      option="${restArgs[i]}"
      if ((i + 1 >= ${#restArgs[@]})) || [[ -z "${restArgs[i + 1]}" || "${restArgs[i + 1]}" == -* ]]; then
        echo "ERROR: ${option} requires a value." >&2
        exit 1
      fi
      ((i += 1))
      if [[ "${option}" == --kube-context ]]; then
        KUBE_CONTEXT="${restArgs[i]}"
      else
        KUBECONFIG_FILE="${restArgs[i]}"
      fi
      ;;
    --kube-context=*|--kubeconfig=*)
      option="${restArgs[i]%%=*}"
      value="${restArgs[i]#*=}"
      if [[ -z "${value}" ]]; then
        echo "ERROR: ${option} requires a value." >&2
        exit 1
      fi
      if [[ "${option}" == --kube-context ]]; then
        KUBE_CONTEXT="${value}"
      else
        KUBECONFIG_FILE="${value}"
      fi
      ;;
  esac
done
KUBECTL_CONTEXT_ARGS=()
if [[ -n "${KUBECONFIG_FILE}" ]]; then
  KUBECTL_CONTEXT_ARGS+=(--kubeconfig "${KUBECONFIG_FILE}")
  HELM_CONTEXT_OPTIONS+=(--kubeconfig "${KUBECONFIG_FILE}")
fi

if [[ "${CMD}" != template ]]; then
  if [[ -z "${KUBE_CONTEXT}" ]]; then
    KUBE_CONTEXT=$(kubectl "${KUBECTL_CONTEXT_ARGS[@]}" config current-context) || exit 1
  fi
  KUBECTL_CONTEXT_ARGS+=(--context "${KUBE_CONTEXT}")
  HELM_CONTEXT_OPTIONS+=(--kube-context "${KUBE_CONTEXT}")
  if ! kubectl "${KUBECTL_CONTEXT_ARGS[@]}" get --raw=/version --request-timeout=10s >/dev/null; then
    echo "ERROR: Kubernetes is unreachable. Start a cluster and select its kubectl context; see dockerfiles/README.md." >&2
    exit 1
  fi
  echo "Kubernetes context: ${KUBE_CONTEXT}"
  if [[ "${KUBE_CONTEXT}" == kind-* && -z "${PERSIST}" ]]; then
    KIND_NODES=$(kubectl "${KUBECTL_CONTEXT_ARGS[@]}" get nodes -o name) || exit 1
    if [[ -n "${KIND_NODES}" && "${KIND_NODES}" != *$'\n'* ]]; then
      echo "Using single-node kind storage settings"
      KIND_OPTIONS=(-f "${scriptdir}/kind-values.yaml")
    fi
  fi
  if [[ "${KUBE_CONTEXT}" == kind-* && "${LOCAL_IMAGE:-0}" == 1 ]]; then
    KIND_BIN=$(command -v kind || true)
    if [[ -z "${KIND_BIN}" && -x "${HOME}/.local/bin/kind" ]]; then
      KIND_BIN="${HOME}/.local/bin/kind"
    fi
    if [[ -z "${KIND_BIN}" ]]; then
      echo "ERROR: kind is required to load the local image into ${KUBE_CONTEXT}. Install kind or supply -l <tag> for an image already available to the cluster." >&2
      exit 1
    fi
    IMAGE="${DOCKER_REPO}/platform-core:${LABEL}"
    case "${DOCKER_REPO%%/*}" in
      *.*|*:*|localhost) ;;
      *) IMAGE="docker.io/${IMAGE}" ;;
    esac
    container_tag "${DOCKER_REPO}/platform-core:${LABEL}" "${IMAGE}" || exit 1
    echo "Loading ${IMAGE} into ${KUBE_CONTEXT}"
    KIND_IMAGE_ARCHIVE=$(mktemp) || exit 1
    trap 'rm -f -- "${KIND_IMAGE_ARCHIVE}"' EXIT
    container_image_save "${IMAGE}" "${KIND_IMAGE_ARCHIVE}" || exit 1
    KIND_EXPERIMENTAL_PROVIDER="${CONTAINER_RUNTIME}" "${KIND_BIN}" load image-archive "${KIND_IMAGE_ARCHIVE}" --name "${KUBE_CONTEXT#kind-}" || exit 1
    rm -f -- "${KIND_IMAGE_ARCHIVE}"
    trap - EXIT
  fi
elif [[ -n "${KUBE_CONTEXT}" ]]; then
  HELM_CONTEXT_OPTIONS+=(--kube-context "${KUBE_CONTEXT}")
fi

if [[ "${CMD}" = "upgrade" ]]; then
  restArgs+=(--install)
fi

HELM_RELEASE_OPTIONS=("${restArgs[@]}" "${HELM_CONTEXT_OPTIONS[@]}")
LOCALFILE_OPTIONS=("${HELM_CONTEXT_OPTIONS[@]}")
if [[ "${CMD}" = "upgrade" ]]; then
  LOCALFILE_OPTIONS+=(--install)
fi

if [[ -n ${PERSIST} ]] ; then
  PERSIST=$(realpath -q $PERSIST || echo $PERSIST)
  PERSIST_PATH=$(echo $PERSIST | sed 's/\\//g')
  if [[ -n "${WSL_DISTRO_NAME}" ]] ; then
    WSLPATH=${PERSIST_PATH#/run/desktop/mnt/host/}
    if [[ ${PERSIST_PATH} != ${WSLPATH} ]] ; then # if they're different PERSIST_PATH was prefixed with WSLPATH
      PERSIST_PATH="/mnt/${WSLPATH}"
      echo WSLPATH = ${PERSIST_PATH}
    fi
  fi
  if [[ $MKDIR == '1' ]] ; then
    for subdir in `grep subPath: $PVFILE | awk '{ print $2 }'` ; do
      echo mkdir -p ${PERSIST_PATH}/$subdir
      mkdir -p ${PERSIST_PATH}/$subdir
    done
  fi
  helm ${CMD} localfile $scriptdir/../helm/examples/local/hpcc-localfile "${LOCALFILE_OPTIONS[@]}" --set common.hostpath=${PERSIST} $PERSISTVALUES | tee lsfull.yaml | grep -A1000 storage: > localstorage.yaml && \
  grep "##" lsfull.yaml  && \
  helm ${CMD} $CLUSTERNAME $scriptdir/../helm/hpcc/ --set global.image.root="${DOCKER_REPO}" --set global.image.version=$LABEL $DEVELOPER_OPTIONS "${HELM_RELEASE_OPTIONS[@]}" -f localstorage.yaml ${PROMETHEUS_METRICS_SINK_ARG} ${ELASTIC_LOG_ACCESS_ARG} --set global.logging.format=${LOG_FORMAT_ARG} || exit 1
else
  echo helm ${CMD} $CLUSTERNAME $scriptdir/../helm/hpcc/ --set global.image.root="${DOCKER_REPO}" --set global.image.version=$LABEL $DEVELOPER_OPTIONS "${KIND_OPTIONS[@]}" "${HELM_RELEASE_OPTIONS[@]}" ${PROMETHEUS_METRICS_SINK_ARG} ${ELASTIC_LOG_ACCESS_ARG} --set global.logging.format=${LOG_FORMAT_ARG}
  helm ${CMD} $CLUSTERNAME $scriptdir/../helm/hpcc/ --set global.image.root="${DOCKER_REPO}" --set global.image.version=$LABEL $DEVELOPER_OPTIONS "${KIND_OPTIONS[@]}" "${HELM_RELEASE_OPTIONS[@]}" ${PROMETHEUS_METRICS_SINK_ARG} ${ELASTIC_LOG_ACCESS_ARG} --set global.logging.format=${LOG_FORMAT_ARG} || exit 1
fi

if [[ -n $ELASTIC_LOG_ACCESS_ARG ]] ; then
  echo -e "\n\nDeploying "myelastic4hpcclogs" - light-weight Elastic Stack:"
  if [[ $DEP_UPDATE == 1 ]]; then
    dependency_update $MANAGED_ELK_SUBPATH
  else
    dependency_check $MANAGED_ELK_SUBPATH
  fi
  helm ${CMD} myelastic4hpcclogs $scriptdir/../helm/$MANAGED_ELK_SUBPATH "${HELM_RELEASE_OPTIONS[@]}" || exit 1
fi

if [[ $DEPLOY_PROM == 1 ]] ; then
  echo -e "\n\nDeploying "myprometheus4hpccmetrics" - Prometheus Stack:"
  if [[ $DEP_UPDATE == 1 ]]; then
    dependency_update $MANAGED_PROM_SUBPATH
  else
    dependency_check $MANAGED_PROM_SUBPATH
  fi
  helm ${CMD} myprometheus4hpccmetrics $scriptdir/../helm/$MANAGED_PROM_SUBPATH "${HELM_RELEASE_OPTIONS[@]}" --set kube-prometheus-stack.prometheus.service.type=LoadBalancer --set kube-prometheus-stack.grafana.service.type=LoadBalancer || exit 1
fi

if [[ $DEPLOY_LOKI == 1 ]] ; then
  echo -e "\n\nDeploying "myloki4hpcclogs" - Grafana Loki Stack:"
  echo -e "\n Access Grafana on: http://localhost:3000 from outside the cluster"
  echo -e "\n  or http://myloki-grafana.default.svc.cluster.local:3000 from within cluster"
  echo -e "\n user: admin"
  echo -e "\n To obtain pass: 'kubectl get secret myloki4hpcclogs-grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo' "
  echo -e "\n Find logs on 'Explore' view, under 'loki' datasource\n"

  if [[ $DEP_UPDATE == 1 ]]; then
    dependency_update $MANAGED_LOKI_SUBPATH
  else
    dependency_check $MANAGED_LOKI_SUBPATH
  fi
  helm ${CMD} myloki4hpcclogs $scriptdir/../helm/$MANAGED_LOKI_SUBPATH "${HELM_RELEASE_OPTIONS[@]}" || exit 1
fi

if [ ${CMD} != "template" ] ; then
  sleep 1
  kubectl "${KUBECTL_CONTEXT_ARGS[@]}" get pods
fi
