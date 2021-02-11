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

# Use this script to build local images for testing, while leveraging the work already done in the most recent tag
# on the branch that your local branch was based on
#
# Pass in a get commit (that you previously built using this script) if you want to override the default calculation
# of the base build


# NB: INPUT_* may be pre-set as environment variables.

while getopts “d:fhlptn:u:b:” opt; do
  case $opt in
    l) TAGLATEST=1 ;;
    n) CUSTOM_TAG_NAME=$OPTARG ;;
    p) PUSH=1 ;;
    t) INPUT_BUILD_THREADS=$OPTARG ;;
    d) INPUT_DOCKER_REPO=$OPTARG ;;
    u) INPUT_BUILD_USER=$OPTARG ;;
    b) INPUT_BUILD_TYPE=$OPTARG ;;
    f) FORCE=1 ;;
    h) echo "Usage: incr.sh [options]"
       echo "    -d <docker-repo>   Specify the repo to publish images to"
       echo "    -f                 Force build from scratch"
       echo "    -b                 Build type (e.g. Debug / Release)"
       echo "    -h                 Display help"
       echo "    -l                 Tag the images as the latest"
       echo "    -n                 Tag the image with a custom name (e.g. -n HPCC-25285)"
       echo "    -p                 Push images to docker repo"
       echo "    -t <num-threads>   Override the number of build threads"
       echo "    -u <user>          Specify the build user"
       exit
       ;;
  esac
done
shift $(( $OPTIND-1 ))

set -e

# NB: Not used if FORCE build
PREV=$1

DOCKER_REPO=hpccsystems
[[ -n ${INPUT_DOCKER_REPO} ]] && DOCKER_REPO=${INPUT_DOCKER_REPO}
INCR_DOCKER_REPO=${DOCKER_REPO}
BUILD_TYPE=Debug
[[ -n ${INPUT_BUILD_TYPE} ]] && BUILD_TYPE=${INPUT_BUILD_TYPE}
BUILD_THREADS=$INPUT_BUILD_THREADS # If not set, picks up default based on nproc

# BUILD_USER used when building from scatch with force option (-f)
BUILD_USER=hpccsystems
[[ -n ${INPUT_BUILD_USER} ]] && BUILD_USER=${INPUT_BUILD_USER}

HEAD=$(git rev-parse --short HEAD)
BUILD_LABEL="${HEAD}-${BUILD_TYPE}"

if [[ -z "$FORCE" ]] ; then
  # Look for an image that matches a commit, exclude images based on dirty working tree.
  if [[ -z ${PREV} ]] ; then
    docker images ${DOCKER_REPO}/platform-build --format {{.Tag}} | egrep -ve '-dirty.*$' > .candidate-tags || true
    PREV=$(git log --format=format:%h-${BUILD_TYPE} $(git describe --abbrev=0 --tags)..HEAD | fgrep -f .candidate-tags | head -n 1)
    rm -f .candidate-tags
    PREV_COMMIT=$(echo "${PREV}" | sed -e "s/-${BUILD_TYPE}.*$//")
  fi

  # If not found above, look for latest tagged
  if [[ -z ${PREV} ]] ; then
    PREV=$(git log --oneline --no-merges | grep "^[0-9a-f]* Split off " | head -1 | sed "s/.*Split off //" | head -1)-rc1
    if [[ ! "${BUILD_TYPE}" =~ "Release" ]] ; then
      PREV=${PREV}-${BUILD_TYPE}
    fi
    echo Finding image based on most recent git tag: ${PREV}
    INCR_DOCKER_REPO=hpccsystems
    docker pull ${INCR_DOCKER_REPO}/platform-build:${PREV}
    if [ $? -ne 0 ]; then
      echo "Could not locate docker image based on PREV tag: ${PREV} for docker user: ${DOCKER_REPO}"
      exit
    fi
  fi

  if [[ -z ${PREV_COMMIT} ]] ; then
    PREV_COMMIT=community_$(echo "${PREV}" | sed -e "s/-${BUILD_TYPE}.*$//")
  fi

  # create empty patch file
  echo -n > platform-build-incremental/hpcc.gitpatch
  porcelain=$(git status -uno --porcelain)
  if [[ -n "${porcelain}" || "${HEAD}" != "${PREV_COMMIT}" ]] ; then
    echo "git diff --binary ${PREV_COMMIT} ':!./' > platform-build-incremental/hpcc.gitpatch"
    git diff --binary ${PREV_COMMIT} ':!./' > platform-build-incremental/hpcc.gitpatch
    # PATCH_MD5 is an ARG of the docker file, which ensures that if different from cached version, image will rebuild from that stage
    PATCH_MD5=$(md5sum platform-build-incremental/hpcc.gitpatch  | awk '{print $1}')
    if [[ -n "${porcelain}" ]] ; then
      DIRTY="-dirty-${PATCH_MD5}"
    fi
    # If working tree is dirty, annotate build tag, so doesn't conflict with base commit
    BUILD_LABEL="${BUILD_LABEL}${DIRTY}"
  elif [[ "$BUILD_LABEL" == "$PREV" ]] ; then
    echo Checking if docker image ${DOCKER_REPO}/platform-core:${BUILD_LABEL} already exists
    EXISTING_IMAGE_LABEL=$(git log --format=format:%h-${BUILD_TYPE} $(git describe --abbrev=0 --tags)..HEAD | grep `docker images ${DOCKER_REPO}/platform-build --format {{.Tag}} | head -n 2 | tail -n 1`)
    if [[ -n ${EXISTING_IMAGE_LABEL} ]] ; then
      PREV=${EXISTING_IMAGE_LABEL}
      PREV_COMMIT=$(echo "${PREV}" | sed -e "s/-${BUILD_TYPE}//")
    fi
  fi
fi

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd $DIR 2>&1 > /dev/null


build_image() {
  local name=$1
  local dockerfolder=$2
  local tags=("${BUILD_LABEL}")
  # if 2nd arg. present, it names the docker folder, otherwise same as $name
  [[ -z ${dockerfolder} ]] && dockerfolder=$name
  local label=$BUILD_LABEL

  if ! docker pull ${DOCKER_REPO}/${name}:${label} ; then
    docker image build -t ${DOCKER_REPO}/${name}:${label} \
        --build-arg DOCKER_REPO=${DOCKER_REPO} \
        --build-arg BUILD_LABEL=${BUILD_LABEL} \
        --build-arg BUILD_TYPE=${BUILD_TYPE} ${@:3} \
        ${dockerfolder}/ 
  fi

  if [ "$TAGLATEST" = "1" ]; then
    local tag="latest"
    docker tag ${DOCKER_REPO}/${name}:${label} ${DOCKER_REPO}/${name}:${tag}
    tags+=("$tag")
  fi

  if [ -n "$CUSTOM_TAG_NAME" ]; then
    local tag="$CUSTOM_TAG_NAME"
    docker tag ${DOCKER_REPO}/${name}:${label} ${DOCKER_REPO}/${name}:${tag}
    tags+=("$tag")
  fi

  if [ "$PUSH" = "1" ] ; then
    for tag in "${tags[@]}";
    do
      docker push ${DOCKER_REPO}/${name}:${tag}
      echo "${tag} pushed"
    done
  fi
}

if [[ -n "$FORCE" ]] ; then
  echo Building local forced build images [ BUILD_LABEL=${BUILD_LABEL} ]
  build_image platform-build platform-build --build-arg BUILD_USER=${BUILD_USER} --build-arg BUILD_TAG=${HEAD} --build-arg BUILD_THREADS=${BUILD_THREADS}
else
  echo Building local incremental images [ BUILD_LABEL=${BUILD_LABEL} ] based on ${PREV}
  build_image platform-build platform-build-incremental --build-arg DOCKER_REPO=${INCR_DOCKER_REPO} --build-arg PREV_LABEL=${PREV} --build-arg PATCH_MD5=${PATCH_MD5} --build-arg BUILD_THREADS=${BUILD_THREADS}
#  rm platform-build-incremental/hpcc.gitpatch
fi

if [[ "${BUILD_TYPE}" =~ "Release" ]] ; then
  echo BUILDING: build_image platform-core
  build_image platform-core
else
  echo BUILDING: build_image platform-core platform-core-debug
  build_image platform-core platform-core-debug
fi

echo Built ${DOCKER_REPO}/*:${BUILD_LABEL}