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

ARG BUILD_LABEL
ARG DOCKER_REPO
FROM ${DOCKER_REPO}/platform-build:${BUILD_LABEL} as build
FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get clean -y && \
    apt-get autoclean -y && \
    apt-get install -y -f && \
    apt-get autoremove -y && \
    apt-get update -y

RUN apt-get install -y \
  default-jdk \
  g++ \
  openssh-client \
  libapr1 \
  libnuma1 \
  libboost-regex1.71.0 \
  libpython3.8 \
  libmemcached11 \
  libmemcachedutil2 \
  libxslt1.1 \
  locales \
  python \ 
  psmisc \
  r-base-core \
  zip \
  curl

ARG USE_CPPUNIT=1
RUN if [ ${USE_CPPUNIT} -eq 1 ] ; then apt-get install -y libcppunit-1.15-0 ; fi 

# these are developer tools - we may want to move them elsewhere so that they are only in the incremental builds?

RUN apt-get install -y \
    dnsutils \
    nano 

# Set the locale
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8     

COPY --from=build /opt/HPCCSystems /opt/HPCCSystems
COPY --from=build /usr/local/bin/kubectl /usr/local/bin/kubectl

# NB: added in platform-build, but need to re-add as this image is not
# based on it. Only /opt/HPCCSystems is copied over.

RUN groupadd -g 1000 hpcc
RUN useradd -s /bin/bash -r -N -c "hpcc runtime User" -u 999 -g hpcc hpcc
RUN passwd -l hpcc 

RUN mkdir /var/lib/HPCCSystems && chown hpcc:hpcc /var/lib/HPCCSystems
RUN mkdir /var/log/HPCCSystems && chown hpcc:hpcc /var/log/HPCCSystems
RUN mkdir /var/lock/HPCCSystems && chown hpcc:hpcc /var/lock/HPCCSystems
RUN mkdir /var/run/HPCCSystems && chown hpcc:hpcc /var/run/HPCCSystems

USER hpcc
ENV PATH="/opt/HPCCSystems/bin:${PATH}"
ENV HPCC_containerized=1
ENV HPCC_DLLSERVER_PATH=/var/lib/HPCCSystems/queries
