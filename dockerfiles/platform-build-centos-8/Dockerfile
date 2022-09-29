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

FROM tgagor/centos-stream:stream8 AS stage

ENV VCPKG_BINARY_SOURCES="clear;nuget,GitHub,readwrite"
ENV VCPKG_NUGET_REPOSITORY=https://github.com/hpcc-systems/vcpkg

# Build Tools - Mono  ---
RUN yum update -y
RUN yum install -y yum-utils
RUN yum-config-manager --add-repo http://download.mono-project.com/repo/centos/
RUN yum clean all
RUN yum makecache
RUN rpm --import "http://keyserver.ubuntu.com/pks/lookup?op=get&search=0x3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF"
RUN dnf config-manager --add-repo https://download.mono-project.com/repo/centos8-stable.repo
RUN dnf install -y mono-complete 

# Build Tools  ---
RUN yum install -y git curl zip unzip tar python3 libtool autoconf automake
RUN yum group install -y "Development Tools"
RUN dnf -y install gcc-toolset-9-gcc gcc-toolset-9-gcc-c++

RUN echo "source /opt/rh/gcc-toolset-9/enable" >> /etc/bashrc
SHELL ["/bin/bash", "--login", "-c"]

RUN . <(curl https://aka.ms/vcpkg-init.sh -L)
RUN . ~/.vcpkg/vcpkg-init fetch cmake
RUN ln -s `. ~/.vcpkg/vcpkg-init fetch cmake | tail -n 1` /usr/local/bin/
RUN ln -s `. ~/.vcpkg/vcpkg-init fetch node | tail -n 1` /usr/local/bin/
RUN ln -s `. ~/.vcpkg/vcpkg-init fetch node | tail -n 1 | sed -e 's!bin/node!bin/npm!g'` /usr/local/bin/

# Libraries  ---
RUN yum remove -y java-1.8*
RUN yum install -y \
    curl-devel \
    python3-devel \
    ncurses-devel \
    openldap-devel \
    numactl-devel \
    java-11-openjdk-devel \
    sqlite-devel \
    libevent-devel \
    libicu-devel \
    mysql-devel

RUN dnf --enablerepo=powertools -y install libmemcached-devel

WORKDIR /hpcc-dev

ARG GITHUB_ACTOR=hpcc-systems
ARG BUILD_TAG=candidate-8.8.0
RUN echo GITHUB_ACTOR is ${GITHUB_ACTOR}
RUN git clone --no-checkout https://github.com/${GITHUB_ACTOR}/HPCC-Platform.git

WORKDIR /hpcc-dev/HPCC-Platform
RUN git checkout ${BUILD_TAG} && \
    git submodule update --init --recursive

WORKDIR /hpcc-dev/HPCC-Platform/vcpkg
RUN ./bootstrap-vcpkg.sh
ARG GITHUB_TOKEN=none
RUN mono `. ~/.vcpkg/vcpkg-init fetch nuget | tail -n 1` \
    sources add \
    -name "GitHub" \
    -source "https://nuget.pkg.github.com/hpcc-systems/index.json" \
    -storepasswordincleartext \
    -username "${GITHUB_ACTOR}" \
    -password "${GITHUB_TOKEN}"

WORKDIR /hpcc-dev
RUN mkdir build
WORKDIR /hpcc-dev/build

ARG BUILD_TYPE=RelWithDebInfo
ARG USE_CPPUNIT=0
RUN cmake /hpcc-dev/HPCC-Platform -Wno-dev \
    -DCONTAINERIZED=OFF \
    -DUSE_OPTIONAL=OFF \
    -DUSE_AZURE=OFF \
    -DINCLUDE_PLUGINS=ON \
    -DSUPPRESS_V8EMBED=ON \
    -DSUPPRESS_REMBED=ON \
    -DUSE_CPPUNIT=${USE_CPPUNIT}

ARG BUILD_THREADS
RUN if [ -n "${BUILD_THREADS}" ] ; then echo ${BUILD_THREADS} > ~/build_threads; else echo $(nproc) > ~/build_threads ; fi
RUN echo Building with $(cat ~/build_threads) threads
RUN cmake --build . -- -j$(cat ~/build_threads) jlib
RUN cmake --build . -- -j$(cat ~/build_threads) esp
RUN cmake --build . -- -j$(cat ~/build_threads) roxie
RUN cmake --build . -- -j$(cat ~/build_threads) ws_workunits ecl
RUN cmake --build . -- -j$(cat ~/build_threads)

# Speed up GH Action
FROM tgagor/centos-stream:stream8
COPY --from=stage /hpcc-dev/build /hpcc-dev/build

