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

# Clean up old images. Useful if your Docker disk gets full...

[[ "$1" == "-all" ]] && docker rm $(docker ps -q -f 'status=exited')
docker rmi $(docker images -q -f "dangling=true")

HEAD=$(git rev-parse --short HEAD)
PREV=$(git describe --abbrev=0 --tags)
for f in `docker images  --format "{{.Repository}}:{{.Tag}}" | grep hpccsystems/ | grep -v $HEAD | grep -v $PREV` ; do
  docker rmi $f
done
