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
# MORE - some of these dependencies are probably not needed by all derived containers - perhaps we should move them
# Others may not be wanted at all in container mode - xterm,sshserver for example??

ARG BUILD_LABEL
FROM hpccsystems/platform-build:${BUILD_LABEL} as build
FROM ubuntu:18.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt clean && \
    apt autoclean && \
    apt install -f && \
    apt autoremove && \
    apt-get update

RUN apt-get install -y \
  xterm \
  g++ \
  openssh-client \
  openssh-server \
  expect \
  rsync \
  libapr1 \
  python \ 
  psmisc \
  curl
  
RUN apt-get install -y \
  libnuma1 \
  libboost-regex1.65.1 \
  libpython3.6 \
  libmemcached11 \
  libmemcachedutil2

RUN apt-get install -y \
  libxslt1.1

RUN apt-get install -y \
    dnsutils \
    nano 

RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.17.0/bin/linux/amd64/kubectl && chmod +x ./kubectl && mv ./kubectl /usr/local/bin

COPY --from=build /opt/HPCCSystems /opt/HPCCSystems

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
