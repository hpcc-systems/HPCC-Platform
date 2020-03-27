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

LABEL=$1
[[ -z ${LABEL} ]] && LABEL=$(docker image ls | fgrep 'hpccsystems/platform-core' | head -n 1 | awk '{print $2}')

helm install mycluster hpcc/ --set global.image.version=$LABEL --set global.privileged=true
sleep 1
kubectl get pods

