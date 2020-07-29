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

# Build container image containing all 3rd-party tools required to build HPCC platform

FROM ubuntu:20.04 as build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt clean -y && \
    apt autoclean -y && \
    apt install -y -f && \
    apt autoremove -y && \
    apt-get update -y

RUN apt-get install -y \
  automake \
  autotools-dev \
  binutils-dev \
  bison \
  build-essential \
  curl \
  default-jdk \
  default-libmysqlclient-dev \
  flex \
  libapr1-dev \
  libaprutil1-dev \
  libarchive-dev \
  libatlas-base-dev \
  libblas-dev \
  libboost-regex-dev \
  libcppunit-dev \
  libcurl4-openssl-dev \
  libevent-dev \
  libhiredis-dev \
  libiberty-dev \
  libicu-dev \
  libldap2-dev \
  libmemcached-dev \
  libnuma-dev \
  libsqlite3-dev \
  libssl-dev \
  libtbb-dev \
  libtool \
  libv8-dev \
  libxalan-c-dev \
  libxslt1-dev \
  pkg-config \
  python-dev \
  python3-dev \
  r-base-dev \
  r-cran-inline \
  r-cran-rcpp \
  r-cran-rinside \
  rsync \
  psmisc \
  libpopt0 \
  zlib1g-dev

RUN apt-get install -y \
  bash-completion \
  expect \
  git \
  nano \
  valgrind \
  sudo \
  vim \
  gdb \
  software-properties-common \  
  lsb-release \
  jq

RUN curl https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add -
RUN apt-add-repository "deb https://deb.nodesource.com/node_12.x $(lsb_release -sc) main"
RUN apt-get update -y
RUN apt-get install -y nodejs

RUN mkdir /home/temp
WORKDIR /home/temp
RUN curl https://cmake.org/files/v3.16/cmake-3.16.3.tar.gz | tar xvz
WORKDIR /home/temp/cmake-3.16.3/
RUN ./bootstrap
RUN make -j4
RUN make install
WORKDIR /
RUN rm -rf /home/temp
RUN apt-get clean -y

FROM ubuntu:20.04
COPY --from=build / /
