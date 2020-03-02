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

BUILD_LABEL=$1
[[ -z ${BUILD_LABEL} ]] && BUILD_LABEL=$(git rev-parse --short HEAD)

docker image build -t hpccsystems/platform-core:${BUILD_LABEL} --build-arg BUILD_LABEL=${BUILD_LABEL} platform-core-debug/  

docker image build -t hpccsystems/roxie:${BUILD_LABEL} --build-arg BUILD_LABEL=${BUILD_LABEL} roxie/  
docker image build -t hpccsystems/dali:${BUILD_LABEL} --build-arg BUILD_LABEL=${BUILD_LABEL} dali/  
docker image build -t hpccsystems/esp:${BUILD_LABEL} --build-arg BUILD_LABEL=${BUILD_LABEL} esp/  
docker image build -t hpccsystems/eclccserver:${BUILD_LABEL} --build-arg BUILD_LABEL=${BUILD_LABEL} eclccserver/  
docker image build -t hpccsystems/eclagent:${BUILD_LABEL} --build-arg BUILD_LABEL=${BUILD_LABEL} eclagent/  
docker image build -t hpccsystems/toposerver:${BUILD_LABEL} --build-arg BUILD_LABEL=${BUILD_LABEL} toposerver/  
docker image build -t hpccsystems/thormaster:${BUILD_LABEL} --build-arg BUILD_LABEL=${BUILD_LABEL} thormaster/
docker image build -t hpccsystems/thorslave:${BUILD_LABEL} --build-arg BUILD_LABEL=${BUILD_LABEL} thorslave/

echo Built hpccsystems/*:${BUILD_LABEL}

