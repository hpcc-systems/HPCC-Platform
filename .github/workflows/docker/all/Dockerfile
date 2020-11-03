# Prepare build environemnt, install pre-requisites
FROM ubuntu:20.04 as hpccbase

RUN apt-get update
RUN apt-get install -y cmake
RUN apt-get install -y bison flex build-essential binutils-dev libldap2-dev libcppunit-dev libicu-dev
RUN apt-get install -y libxslt1-dev zlib1g-dev libboost-regex-dev libarchive-dev python-dev libv8-dev default-jdk
RUN apt-get install -y libapr1-dev libaprutil1-dev libiberty-dev libhiredis-dev libtbb-dev libxalan-c-dev libnuma-dev
RUN apt-get install -y nodejs libevent-dev libatlas-base-dev libblas-dev python3-dev default-libmysqlclient-dev
RUN apt-get install -y libsqlite3-dev r-base-dev r-cran-rcpp r-cran-rinside r-cran-inline libmemcached-dev
RUN apt-get install -y libcurl4-openssl-dev pkg-config libtool autotools-dev automake libssl-dev


RUN apt-get install -y wget curl

#RUN wget https://github.com/Kitware/CMake/releases/download/v3.18.4/cmake-3.18.4.tar.gz
#RUN tar xvpfz cmake-3.18.4.tar.gz
#RUN cd cmake-3.18.4; ./bootstrap; make -j 12 install

RUN curl -sL https://deb.nodesource.com/setup_15.x | bash -
RUN apt-get install -y nodejs

RUN apt-get install -y git

RUN groupadd -g 1000 hpcc
RUN useradd -s /bin/bash -r -m -N -c "hpcc runtime User" -u 999 -g hpcc hpcc
RUN passwd -l hpcc 

RUN mkdir /hpcc-dev
RUN chown hpcc:hpcc /hpcc-dev
USER hpcc

# Fetch and build hpcc sources

WORKDIR /hpcc-dev
ARG GITHUB_REPO_OWNER=hpcc-systems
ARG BASE_BRANCH=master
RUN echo GITHUB_REPO_OWNER is ${GITHUB_REPO_OWNER}
RUN git clone https://github.com/${GITHUB_REPO_OWNER}/HPCC-Platform.git && \
    cd HPCC-Platform && \
    git checkout ${BASE_BRANCH} && \
    git submodule update --init --recursive

ARG BUILD_THREADS
RUN if [ -n "${BUILD_THREADS}" ] ; then echo ${BUILD_THREADS} > ~/build_threads; else echo $(nproc) > ~/build_threads ; fi
RUN echo Building with $(cat ~/build_threads) threads

RUN mkdir /hpcc-dev/build
WORKDIR /hpcc-dev/build

ARG BUILD_TYPE=RelWithDebInfo
ARG USE_CPPUNIT=0

#RUN cmake /hpcc-dev/HPCC-Platform -Wno-dev -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off
RUN mkdir /hpcc-dev/hpccinstall
RUN cmake /hpcc-dev/HPCC-Platform -Wno-dev -DDESTDIR=/hpcc-dev/hpccinstall -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off

RUN make -j$(cat ~/build_threads) jlib
RUN make -j$(cat ~/build_threads) esp
RUN make -j$(cat ~/build_threads) roxie
RUN make -j$(cat ~/build_threads) ws_workunits ecl
RUN make -j$(cat ~/build_threads) thormaster_lcr thorslave_lcr
RUN make -j$(cat ~/build_threads) eclccserver hthor agentexec
RUN make -j$(cat ~/build_threads)

# Prepare build environemnt, install pre-requisites

ARG BUILD_BRANCH=master
ARG GITHUB_ACTOR=hpcc-systems
WORKDIR /hpcc-dev/HPCC-Platform
RUN git fetch --depth=1 https://github.com/${GITHUB_ACTOR}/HPCC-Platform ${BUILD_BRANCH}:${BUILD_BRANCH}
RUN git checkout ${BUILD_BRANCH}
RUN git submodule update --init --recursive

WORKDIR /hpcc-dev/build

ARG BUILD_TYPE=RelWithDebInfo
ARG USE_CPPUNIT=0

#RUN cmake /hpcc-dev/HPCC-Platform -Wno-dev -DINCLUDE_PLUGINS=0 -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PYTHON2=0 -DUSE_PYTHON3=0 -DSUPPRESS_SPARK=1 -DUSE_CPPUNIT=${USE_CPPUNIT} -DUSE_CASSANDRA=Off

RUN make -j$(cat ~/build_threads) jlib
RUN make -j$(cat ~/build_threads) esp
RUN make -j$(cat ~/build_threads) roxie
RUN make -j$(cat ~/build_threads) ws_workunits ecl
RUN make -j$(cat ~/build_threads) thormaster_lcr thorslave_lcr
RUN make -j$(cat ~/build_threads) eclccserver hthor agentexec
RUN make -j$(cat ~/build_threads)
RUN make install

# For debugging image only
USER root
RUN apt-get install -y sudo
RUN usermod -aG sudo hpcc
RUN echo "hpcc     ALL=(ALL) NOPASSWD:ALL"  >> /etc/sudoers
USER hpcc

WORKDIR /hpcc-dev
COPY ./entrypoint.sh .

WORKDIR /hpcc-dev/HPCC-Platform/testing/regress
COPY ./ecl-test.json .

ENTRYPOINT ["/hpcc-dev/entrypoint.sh"]
