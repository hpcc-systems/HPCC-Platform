##############################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.
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

# Base container image that builds all HPCC platform components

ARG BASE_VER=8.6 
ARG CR_USER=hpccsystems
ARG CR_REPO=docker.io
ARG CR_CONTAINER_NAME=platform-build-base
FROM ${CR_REPO}/${CR_USER}/${CR_CONTAINER_NAME}:${BASE_VER}

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt-get install -y dirmngr gnupg apt-transport-https ca-certificates software-properties-common
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF
RUN apt-add-repository 'deb https://download.mono-project.com/repo/ubuntu stable-focal main'
RUN apt install -y mono-complete 

ARG GITHUB_OWNER=hpcc-systems
ARG GITHUB_TOKEN=none
RUN echo REPO_OWNER is ${REPO_OWNER}
RUN echo BUILD_BRANCH is ${BUILD_BRANCH}

ENV VCPKG_BINARY_SOURCES="clear;nuget,GitHub,readwrite"
ENV VCPKG_NUGET_REPOSITORY=https://github.com/hpcc-systems/vcpkg

COPY ./vcpkg /tmp
WORKDIR /tmp
RUN ./bootstrap-vcpkg.sh

RUN mono `./vcpkg fetch nuget | tail -n 1` \
    sources add \
    -name "GitHub" \
    -source "https://nuget.pkg.github.com/hpcc-systems/index.json" \
    -storepasswordincleartext \
    -username "${GITHUB_OWNER}" \
    -password "${GITHUB_TOKEN}"

WORKDIR /build 

CMD cmake /src -DCMAKE_TOOLCHAIN_FILE=/src/vcpkg/scripts/buildsystems/vcpkg.cmake -DVCPKG_OVERLAY_PORTS=../src/vcpkg-overlays -DUSE_OPTIONAL=OFF -DCMAKE_BUILD_TYPE=Debug -DINCLUDE_PLUGINS=ON -DSUPPRESS_V8EMBED=ON -DSUPPRESS_REMBED=ON -DSUPPRESS_COUCHBASEEMBED=ON
