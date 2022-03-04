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
ARG DEST_DOCKER_REPO
FROM ${DEST_DOCKER_REPO}/platform-build-ln:${BUILD_LABEL} as plugins
FROM ${DOCKER_REPO}/platform-core:${BUILD_LABEL}
ENV DEBIAN_FRONTEND=noninteractive

USER root
COPY --from=plugins /opt/HPCCSystems/ /opt/HPCCSystems/
USER hpcc
ARG BUILD_TAG_OVERRIDE
ENV HPCC_BUILD_TAG=${BUILD_TAG_OVERRIDE}

