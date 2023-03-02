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

ARG BUILD_LABEL
ARG DOCKER_REPO
FROM ${DOCKER_REPO}/platform-build:${BUILD_LABEL}

USER hpcc
WORKDIR /hpcc-dev

ARG BUILD_USER=hpcc-systems
RUN echo BUILD_USER is ${BUILD_USER}
ARG GITHUB_TOKEN
ARG BUILD_TAG=none
RUN git clone https://oauth2:${GITHUB_TOKEN}@github.com/${BUILD_USER}/LN.git && \
    cd LN && \
    git submodule update --init --recursive

WORKDIR /hpcc-dev/LN
RUN git fetch origin && \
    git checkout ${BUILD_TAG} && \
    git submodule update --init --recursive

WORKDIR /hpcc-dev
RUN mkdir lnbuild
WORKDIR /hpcc-dev/lnbuild

ARG BUILD_TYPE=RelWithDebInfo
ARG USE_CPPUNIT=1
RUN cmake /hpcc-dev/LN -Wno-dev -DCONTAINERIZED=1 -DINCLUDE_PLUGINS=1 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_CPPUNIT=${USE_CPPUNIT} -DINCLUDE_EE_PLUGINS=1

ARG BUILD_THREADS
RUN if [ -n "${BUILD_THREADS}" ] ; then echo ${BUILD_THREADS} > ~/build_threads; else echo $(nproc) > ~/build_threads ; fi
RUN echo Building with $(cat ~/build_threads) threads
RUN make -j$(cat ~/build_threads) plugins
RUN make -j$(cat ~/build_threads) mysqllogagent wslogserviceespagent

USER root

RUN cd plugins && make -j$(cat ~hpcc/build_threads) install
RUN cd oss/plugins/eeproxies && make -j$(cat ~hpcc/build_threads) install
RUN cd system/mysql && make -j$(cat ~hpcc/build_threads) install
RUN cd esp/logging/loggingagent/mysqlloggingagent && make -j$(cat ~hpcc/build_threads) install
RUN cd esp/logging/loggingagent/wslogserviceespagent && make -j$(cat ~hpcc/build_threads) install
