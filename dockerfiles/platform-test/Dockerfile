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
# Others may not be wanted at all in container mode - scikit example??

ARG BUILD_LABEL=
ARG DOCKER_REPO=hpccsystems
FROM ${DOCKER_REPO}/platform-core:${BUILD_LABEL}
#FROM hpccsystems/platform-core:9.8.44

ARG GITHUB_TOKEN=
ARG REPO_BRANCH=
ARG TEST_REPO="cloud-testing.git"
ENV GIT_BRANCH=${REPO_BRANCH}
ENV GIT_TEST_REPO=${TEST_REPO}
USER root

# Machine Learning Dependencies
RUN apt-get update && apt-get install -y python3 python3-pip --fix-missing
RUN apt-get install -y libsvm-dev libsvm-tools bc wget vim
RUN python3 -m pip install --upgrade pip
RUN pip3 install requests

# Install Selenium
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb
RUN rm -rf ./google-chrome-stable_current_amd64.deb
RUN pip3 install selenium
RUN pip3 install webdriver-manager
RUN pip3 install requests

USER hpcc
RUN git config --global http.version HTTP/2 && \
    cd /home/hpcc && git clone https://github.com/hpcc-systems/HPCC-Platform.git

#    git config --global http.postBuffer 524288000 && \
#RUN cd /home/hpcc/HPCC-Platform  &&  git checkout community_9.8.44-1
RUN echo "git checkout ${GIT_BRANCH}"
RUN cd /home/hpcc/HPCC-Platform && git checkout ${GIT_BRANCH}

RUN cd /home/hpcc && git clone https://oauth2:${GITHUB_TOKEN}@github.com/hpcc-systems/${GIT_TEST_REPO}

RUN mkdir -p /home/hpcc/test/BVT
RUN mkdir -p /home/hpcc/tmp
RUN cp /home/hpcc/cloud-testing/BVT/bvt.sh /home/hpcc/test/BVT/
RUN cp -r /home/hpcc/cloud-testing/ECLWatch/playground /home/hpcc/test/
RUN rm -rf /home/hpcc/cloud-testing

RUN mkdir -p /home/hpcc/test/data
RUN cd /home/hpcc/test/data && wget https://cdn.hpccsystems.com/install/docs/3_8_0_8rc_CE/OriginalPerson
