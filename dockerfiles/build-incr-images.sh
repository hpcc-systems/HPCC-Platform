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

# Utility script used by incr.sh and incr-patch.sh
# New build label should be given as single argument

BUILD_LABEL=$1

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

