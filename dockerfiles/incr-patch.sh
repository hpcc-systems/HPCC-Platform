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
# This script creates a git patch based on the differences between the most recent build and the current working
# directory state and uses that to patch most recent version.
#
# Pass in a get commit (that you previously built using this script) if you want to override the default calculation
# of the base build

HEAD=$(git rev-parse --short HEAD)
PREV=$1
[[ -z ${PREV} ]] && PREV=$(git log --format=format:%h $(git describe --abbrev=0 --tags)..HEAD | grep `docker images hpccsystems/platform-build --format {{.Tag}} | head -n 1`)
[[ -z ${PREV} ]] && PREV=$(git describe --abbrev=0 --tags)

PREV_LABEL="${PREV}-Debug"

BUILD_TYPE=Debug
BUILD_LABEL=${HEAD}-Debug

if [[ "$BUILD_LABEL" == "${PREV_LABEL}" ]] ; then
    echo Docker image hpccsystems/platform-core:${HEAD} already exists
    PREV=$(git log --format=format:%h $(git describe --abbrev=0 --tags)..HEAD | grep `docker images hpccsystems/platform-build --format {{.Tag}} | head -n 2 | tail -n 1`)
    [[ -z ${PREV} ]] && PREV=$(git describe --abbrev=0 --tags)
fi

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd $DIR 2>&1 > /dev/null

echo Building local incremental images based on ${PREV_LABEL}
    
if [[ -n "$FORCE" ]] ; then
  docker image build -t hpccsystems/platform-build:${BUILD_LABEL} --build-arg PREV_LABEL=${HEAD}-Debug --build-arg BASE_VER=7.8 --build-arg BUILD_TYPE=Debug platform-build/
else
  git format-patch ${PREV} --stdout > platform-build-patch/hpcc.gitpatch
  docker image build -t hpccsystems/platform-build:${BUILD_LABEL} --build-arg PREV_LABEL=${PREV_LABEL} platform-build-patch/
  rm platform-build-patch/hpcc.gitpatch
fi

./build-incr-images.sh ${BUiLD_LABEL}

echo Built hpccsystems/*:${BUILD_LABEL}

