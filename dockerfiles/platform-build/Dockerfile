ARG BASE_VER=7.8
FROM hpccsystems/platform-build-base:${BASE_VER}

WORKDIR /hpcc-dev
RUN git clone https://github.com/hpcc-systems/HPCC-Platform.git && \
    cd HPCC-Platform && \
    git submodule update --init --recursive

WORKDIR /hpcc-dev/HPCC-Platform
ARG BUILD_VER=master
RUN git fetch origin && \
    git checkout ${BUILD_VER} && \
    git submodule update --init --recursive

WORKDIR /hpcc-dev
RUN mkdir build
WORKDIR /hpcc-dev/build

RUN JAVA_HOME=`/usr/libexec/java_home` cmake /hpcc-dev/HPCC-Platform -Wno-dev 

RUN make -j$(nproc) jlib
RUN make -j$(nproc) esp
RUN make -j$(nproc) roxie
RUN make  ws_workunits  ecl
RUN make -j$(nproc)
RUN make -j$(nproc) package
