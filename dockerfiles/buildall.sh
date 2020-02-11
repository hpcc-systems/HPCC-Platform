#!/bin/bash

#todo - trim the cmake artefacts out of the base build
 
# Work out which candidate branch this one was probably forked from

git_parent() {
  best=1000000
  parent=
  for candidate in `git branch | grep ' candidate-' | sed 's/\*//'` master ; do
    delta=$(git rev-list --count --no-merges $candidate...HEAD)
    if [ $delta -lt $best ] ; then
      best=$delta
      parent=$candidate
    fi
  done
  if [ -z $parent ] ; then
    exit 2
  fi
  echo $parent
}

#eval $(minikube docker-env)

BASE_VER=7.8
BUILD_VER=master-2020-02-10
HEAD=$(git rev-parse --short HEAD)
PARENT=`git_parent`
PUSH=1

set -e

#docker rm $(docker ps -q -f 'status=exited')
#docker rmi $(docker images -q -f "dangling=true")

if ! docker pull hpccsystems/platform-build-base:${BASE_VER} ; then
  docker image build -t hpccsystems/platform-build-base:${BASE_VER} platform-build-base/ 
  if [ "$PUSH" = "1" ] ; then
    docker push hpccsystems/platform-build-base:${BASE_VER}
  fi
fi

if ! docker pull hpccsystems/platform-build:${BUILD_VER} ; then
  docker image build -t hpccsystems/platform-build:${BUILD_VER} --build-arg BASE_VER=${BASE_VER} --build-arg BUILD_VER=${BUILD_VER} platform-build/  
  if [ "$PUSH" = "1" ] ; then
    docker push hpccsystems/platform-build:${BUILD_VER}
  fi
fi

if ! docker pull hpccsystems/platform-core:${BUILD_VER} ; then
  docker image build -t hpccsystems/platform-core:${BUILD_VER} --build-arg BUILD_VER=${BUILD_VER} platform-core/  
  if [ "$PUSH" = "1" ] ; then
    docker push hpccsystems/platform-core:${BUILD_VER}
  fi
fi

#more - only do this if head does not match BUILD_VER
docker image build -t hpccsystems/platform-build:${HEAD} --build-arg BUILD_VER=${BUILD_VER} --build-arg COMMIT=${HEAD} platform-build-incremental/
docker image build -t hpccsystems/platform-core:${HEAD} --build-arg BUILD_VER=${HEAD} platform-core/  

docker image build -t hpccsystems/roxie:${HEAD} --build-arg BUILD_VER=${HEAD} roxie/
docker image build -t hpccsystems/dali:${HEAD} --build-arg BUILD_VER=${HEAD} dali/
docker image build -t hpccsystems/esp:${HEAD} --build-arg BUILD_VER=${HEAD} esp/
docker image build -t hpccsystems/eclccserver:${HEAD} --build-arg BUILD_VER=${HEAD} eclccserver/
docker image build -t hpccsystems/eclagent:${HEAD} --build-arg BUILD_VER=${HEAD} eclagent/

