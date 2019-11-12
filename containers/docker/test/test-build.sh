#!/usr/bin/env bash
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
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
################################################################################
#
# Run a test build for all images.

source functions

usage()
{
  echo ""
  echo "Usage: test-build.sh -l <linux ariant> -r <repo> -t <tag>"
  echo "   -l linux variant, comma delimited. The default is ubuntu. The other choice is centos"
  echo "   -r Docker repository. The default is hpccsystems/platform. Repo for gitlab, for example ln + plugins:"
  echo "      gitlab.ins.risk.regn.net/docker-images/hpccsystems/ln-platform-wp"
  echo "   -t build tag. The default is latest. "
  echo ""
  exit 1
}


function test()
{
  echo ""
  info "Testing $repo:$tag"
  if [ "$variant" = "centos" ]
  then
      info "docker run --rm -cap-add SYS_RESOURCE -e \"container=docker\" -v \"$PWD/test-${package}.sh:/usr/local/bin/test.sh\" ${repo}:${tag} test.sh"
      sudo docker run --rm -cap-add SYS_RESOURCE -e "container=docker" -v "$PWD/test-${package}.sh:/usr/local/bin/test.sh" ${repo}:${tag} test.sh
  else
      info "docker run --rm -v \"$PWD/test-${package}.sh:/usr/local/bin/test.sh\" ${repo}:${tag} test.sh"
      sudo docker run --rm -v "$PWD/test-${package}.sh:/usr/local/bin/test.sh" ${repo}:${tag} test.sh
  fi
  echo ""
}

package="platform"
variants="ubuntu"
tag=latest
repo="hpccsystems/platform"

while getopts "*l:r:t:" arg
do
  case "$arg" in
       l) IFS=',' read -ra variants <<< "${OPTARG}"
          ;;
       r) repo=${OPTARG}
          ;;
       t) tag=${OPTARG}
          ;;
       ?) usage
          ;;
  esac
done

test
