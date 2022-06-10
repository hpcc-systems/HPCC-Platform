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

ARG BASE_VER=8.6 
ARG CR_USER=hpccsystems
ARG CR_REPO=docker.io
ARG CR_CONTAINER_NAME=platform-build-base
FROM ${CR_REPO}/${CR_USER}/${CR_CONTAINER_NAME}:${BASE_VER}

ENV DEBIAN_FRONTEND=noninteractive
ENV VCPKG_BINARY_SOURCES="clear;files,/vcpkg-cache,readwrite"

COPY ./vcpkg/ /vcpkg/

VOLUME ["/vcpkg-cache"]

WORKDIR /vcpkg

RUN ./bootstrap-vcpkg.sh

CMD ["./vcpkg", "install", "--overlay-ports=./overlays", "--debug"]
