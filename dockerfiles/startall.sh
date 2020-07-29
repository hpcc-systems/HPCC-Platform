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

CMD="install"
while [ "$#" -gt 0 ]; do
  arg=$1
  if [[ ${arg:0:1} == '-' ]]; then
    case "${arg:1:1}" in
      l) shift
         LABEL=$1
         ;;
      d) shift;
         INPUT_DOCKER_REPO=$1
         ;;
      u) CMD="upgrade"
         ;;
      p) shift
         PERSIST=$1
         ;;
      h) echo "Usage: startall.sh [options]"
         echo "    -d <docker-repo>   Docker repository to fetch images from"
         echo "    -l                 Build image label to use"
         echo "    -u                 Use "upgrade" rather than "install"
         echo "    -p <location>      Use local persistent data
         exit
         ;;
      *) restArgs+=(${arg})
         ;;
    esac
  else
    restArgs+=(${arg})
  fi
  shift
done

[[ -n ${INPUT_DOCKER_REPO} ]] && DOCKER_REPO=${INPUT_DOCKER_REPO}
[[ -z ${LABEL} ]] && LABEL=$(docker image ls | fgrep "${DOCKER_REPO}/platform-core" | head -n 1 | awk '{print $2}')

if [[ -n ${PERSIST} ]] ; then
  mkdir -p ${PERSIST}/dlls
  mkdir -p ${PERSIST}/dali
  mkdir -p ${PERSIST}/data
  helm ${CMD} localfile $scriptdir/../helm/examples/local/hpcc-localfile --set common.hostpath=${PERSIST} | grep -A100 storage > localstorage.yaml && \
  helm ${CMD} mycluster $scriptdir/../helm/hpcc/ --set global.image.root="${DOCKER_REPO}" --set global.image.version=$LABEL --set global.privileged=true -f localstorage.yaml ${restArgs[@]}
else
  helm ${CMD} mycluster $scriptdir/../helm/hpcc/ --set global.image.root="${DOCKER_REPO}" --set global.image.version=$LABEL --set global.privileged=true ${restArgs[@]}
fi

sleep 1
kubectl get pods

