#!/usr/bin/env bash
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
