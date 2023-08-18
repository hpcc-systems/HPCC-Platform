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

# Create base container image to be used by all HPCC processes 

ARG BASE_IMAGE=ubuntu:22.04
FROM ${BASE_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get clean -y && \
    apt-get autoclean -y && \
    apt-get install -y -f && \
    apt-get autoremove -y && \
    apt-get update -y && \
    apt-get install --no-install-recommends -y \
    default-jdk \
    elfutils \
    expect \
    g++ \
    git \
    git-lfs \
    locales \
    jq \
    openssh-client \
    openssh-server \
    python3 \ 
    python3-dev \ 
    psmisc \
    r-base-core \
    r-cran-rcpp \
    r-cran-inline \
    rsync \
    zip \
    curl \
    clang

ARG USE_CPPUNIT=1
RUN if [ ${USE_CPPUNIT} -eq 1 ] ; then apt-get install -y libcppunit-1.15-0 ; fi 

# these are developer tools - we may want to move them elsewhere so that they are only in the incremental builds?

RUN apt-get install -y \
    dnsutils \
    gdb \
    nano 

RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.18.18/bin/linux/amd64/kubectl && \
    chmod +x ./kubectl && \
    mv ./kubectl /usr/local/bin

# Set the locale
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8     

RUN groupadd -g 10001 hpcc
RUN useradd -s /bin/bash -m -r -N -c "hpcc runtime User" -u 10000 -g hpcc hpcc
RUN passwd -l hpcc 

RUN mkdir /var/lib/HPCCSystems && chown hpcc:hpcc /var/lib/HPCCSystems
RUN mkdir /var/log/HPCCSystems && chown hpcc:hpcc /var/log/HPCCSystems
RUN mkdir /var/lock/HPCCSystems && chown hpcc:hpcc /var/lock/HPCCSystems
RUN mkdir /var/run/HPCCSystems && chown hpcc:hpcc /var/run/HPCCSystems

ARG PKG_FILE=hpccsystems-platform-community_9.2.4-1jammy_amd64_k8s.deb
COPY ./${PKG_FILE} /tmp/${PKG_FILE}
RUN dpkg -i /tmp/${PKG_FILE} && \
    apt-get install -f && \
    rm /tmp/${PKG_FILE}

USER hpcc
ENV PATH="/opt/HPCCSystems/bin:${PATH}"
ENV HPCC_containerized=1
WORKDIR /var/lib/HPCCSystems
