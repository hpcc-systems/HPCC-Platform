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

# A version of the platform-core Dockerfile that keeps all the build artefacts in place, for use by developers wanting to debug the platform

ARG BUILD_LABEL
FROM hpccsystems/platform-build:${BUILD_LABEL}

RUN apt-get install -y \
    dnsutils \
    nano 

RUN useradd -s /bin/bash -r -U -c "hpcc runtime User" hpcc
RUN passwd -l hpcc 

RUN mkdir /var/lib/HPCCSystems && chown hpcc:hpcc /var/lib/HPCCSystems
RUN mkdir /var/log/HPCCSystems && chown hpcc:hpcc /var/log/HPCCSystems
RUN mkdir /var/lock/HPCCSystems && chown hpcc:hpcc /var/lock/HPCCSystems
RUN mkdir /var/run/HPCCSystems && chown hpcc:hpcc /var/run/HPCCSystems

USER hpcc

ENV PATH="/opt/HPCCSystems/bin:${PATH}"
ENV HPCC_containerized=1
ENV HPCC_DLLSERVER_PATH=/var/lib/HPCCSystems/queries
