##############################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
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

ARG CR_USER=hpccsystems
ARG CR_REPO=docker.io
ARG PLATFORM_PRBASE_VER
FROM ${CR_REPO}/${CR_USER}/platform-build:${PLATFORM_PRBASE_VER}

USER hpcc

WORKDIR /hpcc-dev/HPCC-Platform
ARG GITHUB_REPO
RUN git remote add upstream https://github.com/${GITHUB_REPO}
RUN git fetch upstream +${GITHUB_PRREF}
RUN git checkout FETCH_HEAD
RUN git submodule update --init --recursive

WORKDIR /hpcc-dev/build

ARG BUILD_THREADS
RUN if [ -n "${BUILD_THREADS}" ] ; then echo ${BUILD_THREADS} > ~/build_threads; else echo $(nproc) > ~/build_threads ; fi
RUN echo Building with $(cat ~/build_threads) threads
RUN make -j$(cat ~/build_threads) jlib
RUN make -j$(cat ~/build_threads) esp
RUN make -j$(cat ~/build_threads) roxie
RUN make -j$(cat ~/build_threads) ws_workunits ecl
RUN make -j$(cat ~/build_threads)

USER root

RUN make -j$(cat ~hpcc/build_threads) install
