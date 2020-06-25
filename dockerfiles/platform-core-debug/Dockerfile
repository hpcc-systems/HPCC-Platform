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
ARG DOCKER_REPO
FROM ${DOCKER_REPO}/platform-build:${BUILD_LABEL}

# Set the locale
RUN apt-get install -y locales
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8     

RUN apt-get install -y libcap2-bin zip
RUN setcap cap_sys_ptrace+eip /usr/bin/gdb

ENV PATH="/opt/HPCCSystems/bin:${PATH}"
ENV HPCC_containerized=1
ENV HPCC_DLLSERVER_PATH=/var/lib/HPCCSystems/queries

USER hpcc
