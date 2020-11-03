# Prepare build environemnt, install pre-requisites
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG BUILD_BRANCH=master
ARG GITHUB_ACTOR=hpcc-systems
WORKDIR /hpcc-dev/HPCC-Platform
RUN git fetch https://github.com/${GITHUB_ACTOR}/HPCC-Platform ${BUILD_BRANCH}:${BUILD_BRANCH}
RUN git checkout ${BUILD_BRANCH}
RUN git submodule update --init --recursive

ARG BUILD_THREADS
RUN if [ -n "${BUILD_THREADS}" ] ; then echo ${BUILD_THREADS} > ~/build_threads; else echo $(nproc) > ~/build_threads ; fi
RUN echo Building with $(cat ~/build_threads) threads

WORKDIR /hpcc-dev/build

ARG BUILD_TYPE=RelWithDebInfo
ARG USE_CPPUNIT=0

RUN rm -f CMakeCache.txt && cmake /hpcc-dev/HPCC-Platform -Wno-dev -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off

RUN make -j$(cat ~/build_threads) jlib
RUN make -j$(cat ~/build_threads) esp
RUN make -j$(cat ~/build_threads) roxie
RUN make -j$(cat ~/build_threads) ws_workunits ecl
RUN make -j$(cat ~/build_threads) thormaster_lcr thorslave_lcr
RUN make -j$(cat ~/build_threads) eclccserver hthor agentexec
RUN make -j$(cat ~/build_threads)

