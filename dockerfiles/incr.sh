#!/bin/bash
HEAD=$(git rev-parse --short HEAD)
PREV=$(git rev-parse --short HEAD^)

set -e

#more - only do this if head does not match BUILD_VER
docker image build -t hpccsystems/platform-build:${HEAD} --build-arg BUILD_VER=${PREV} --build-arg COMMIT=${HEAD} platform-build-incremental/
docker image build -t hpccsystems/platform-core:${HEAD} --build-arg BUILD_VER=${HEAD} platform-core/  

docker image build -t hpccsystems/roxie:${HEAD} --build-arg BUILD_VER=${HEAD} roxie/
docker image build -t hpccsystems/dali:${HEAD} --build-arg BUILD_VER=${HEAD} dali/
docker image build -t hpccsystems/esp:${HEAD} --build-arg BUILD_VER=${HEAD} esp/
docker image build -t hpccsystems/eclccserver:${HEAD} --build-arg BUILD_VER=${HEAD} eclccserver/
docker image build -t hpccsystems/eclagent:${HEAD} --build-arg BUILD_VER=${HEAD} eclagent/

