################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
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
################################################################################
FROM hpccsystems/hpcc-base:7-bionic

ARG version=
RUN if [ -z "${version}" ] ; then echo Missing HPCC version; exit 1; else : ; fi

#ENV PLATFORM_DOWNLOAD_MD5  a62b43a1ef1ecf7d8d702d9033e9cf39
RUN set -ex; \
   apt-get update; \
   apt-get install -y iputils-ping curl xterm libarchive13 ansible iputils-ping python3-pip libssl-dev ; \
   pip3 install kubernetes ; \
   groupadd hpcc && useradd -s /bin/bash -r -m -d /home/hpcc -g hpcc -c "hpcc Runtime User" hpcc ; \
   export VERSION_MMP=${version%-*}; \
   export PLATFORM_PACKAGE=hpccsystems-platform-community_${version}bionic_amd64.deb; \
   wget  "http://d2wulyp08c6njk.cloudfront.net/releases/CE-Candidate-${VERSION_MMP}/bin/platform/${PLATFORM_PACKAGE}"; \
   #wget "http://10.240.32.242/builds/CE-Candidate-${VERSION}/bin/platform/${PLATFORM_PACKAGE}"; \
   #wget "http://10.240.32.242/builds/custom/Spark/envgen2/${PLATFORM_PACKAGE}"; \
   #echo "$PLATFORM_DOWNLOAD_MD5 ${PLATFORM_PACKAGE}" | md5sum -c -; \
   dpkg -i "${PLATFORM_PACKAGE}" ;\
   rm -rf "${PLATFORM_PACKAGE}"

EXPOSE 8015 18015

#COPY id_rsa* /home/hpcc/.ssh/
#RUN set -ex; \
#   chmod 400 /home/hpcc/.ssh/id_rsa; \
#   chmod 644 /home/hpcc/.ssh/id_rsa.pub

RUN cp -r /home/hpcc/.ssh ~/
COPY hpcc-tools /opt/hpcc-tools
ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["/usr/sbin/sshd", "-D"]
