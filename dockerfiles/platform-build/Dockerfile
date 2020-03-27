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

# Base container image that builds all HPCC platform components

ARG BASE_VER=7.8
FROM hpccsystems/platform-build-base:${BASE_VER}

RUN groupadd -g 1000 hpcc
RUN useradd -s /bin/bash -r -m -N -c "hpcc runtime User" -u 999 -g hpcc hpcc
RUN passwd -l hpcc 


WORKDIR /hpcc-dev
RUN chown hpcc:hpcc /hpcc-dev

# Runs as hpcc to fetch sources and build, to aid debugging containers as user hpcc
USER hpcc

ARG BUILD_USER=hpcc-systems
RUN echo BUILD_USER is ${BUILD_USER}
RUN git clone https://github.com/${BUILD_USER}/HPCC-Platform.git && \
    cd HPCC-Platform && \
    git submodule update --init --recursive

WORKDIR /hpcc-dev/HPCC-Platform
ARG BUILD_TAG=none
RUN git fetch origin && \
    git checkout ${BUILD_TAG} && \
    git submodule update --init --recursive

WORKDIR /hpcc-dev
RUN mkdir build
WORKDIR /hpcc-dev/build

ARG BUILD_TYPE=RelWithDebInfo
RUN cmake /hpcc-dev/HPCC-Platform -Wno-dev -DCONTAINERIZED=1 -DCMAKE_BUILD_TYPE=${BUILD_TYPE}

RUN make -j$(nproc) jlib
RUN make -j$(nproc) esp
RUN make -j$(nproc) roxie
RUN make -j$(nproc) ws_workunits ecl
RUN make -j$(nproc)

USER root

RUN make -j$(nproc) install
RUN mkdir /var/lib/HPCCSystems && chown hpcc:hpcc /var/lib/HPCCSystems
RUN mkdir /var/log/HPCCSystems && chown hpcc:hpcc /var/log/HPCCSystems
RUN mkdir /var/lock/HPCCSystems && chown hpcc:hpcc /var/lock/HPCCSystems
RUN mkdir /var/run/HPCCSystems && chown hpcc:hpcc /var/run/HPCCSystems

