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

ARG UID=10001
ARG GID=10000
RUN echo hpcc is ${UID}:${GID}

RUN groupadd -g ${UID} hpcc
RUN useradd -s /bin/bash -r -m -N -c "hpcc runtime User" -u ${GID} -g hpcc hpcc
RUN passwd -l hpcc 

RUN apt install -f /build/*.deb

RUN /etc/init.d/hpcc-init start

USER hpcc

CMD top
