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
# Others may not be wanted at all in container mode - tensorflow, etc  ??

ARG BUILD_LABEL
FROM hpccsystems/platform-core:${BUILD_LABEL}
USER root

RUN apt-get install -y python3-pip --fix-missing
RUN python3 -m pip install --upgrade pip

RUN pip3 install       \
    scikit-learn       \
    statsmodels        \
    networkx           \
    setuptools==46.1.3 \
    scipy==1.4.1       \
    tensorflow==2.2.0

USER hpcc
