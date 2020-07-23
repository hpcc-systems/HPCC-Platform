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

# Build container image for a git commit, based on an earlier build
# For use when developing/testing the system only
# See incr.sh for how this is used

ARG PREV_LABEL
ARG DOCKER_REPO
FROM ${DOCKER_REPO}/platform-build:${PREV_LABEL}
RUN apt clean -y && \
    apt autoclean -y && \
    apt install -y -f && \
    apt autoremove -y && \
    apt-get update -y

RUN apt-get install -y \
    dnsutils \
    nano 

# Set the locale
RUN apt-get install -y locales
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8     

USER hpcc
WORKDIR /hpcc-dev/HPCC-Platform

# NB: PATCH_MD5 ensures cache miss (and therefore rebuild) if MD5 is different
ARG PATCH_MD5
COPY --chown=hpcc:hpcc hpcc.gitpatch .
RUN if [ -s hpcc.gitpatch ]; then git apply --whitespace=nowarn hpcc.gitpatch; fi

WORKDIR /hpcc-dev/build
ARG BUILD_THREADS
RUN if [ -n "${BUILD_THREADS}" ] ; then echo ${BUILD_THREADS} > ~/build_threads; else echo $(($(nproc)*3/2)) > ~/build_threads ; fi
RUN echo Building with $(cat ~/build_threads) threads
RUN make -j$(cat ~/build_threads)

USER root
RUN make -j$(cat ~hpcc/build_threads) install

