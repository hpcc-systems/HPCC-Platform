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


RUN set -ex; \
   apt-get update; \
   apt-get install -y curl libarchive13 openjdk-8-jdk; \
   export VERSION_MMP=${version%-*}; \
   export CLIENTTOOLS_PACKAGE=hpccsystems-clienttools-community_${version}bionic_amd64.deb; \
   wget  "http://d2wulyp08c6njk.cloudfront.net/releases/CE-Candidate-${VERSION_MMP}/bin/clienttools/${CLIENTTOOLS_PACKAGE}"; \
   #echo "$CLIENTTOOLS_DOWNLOAD_MD5 ${CLIENTTOOLS_PACKAGE}" | md5sum -c -; \
   dpkg -i "${CLIENTTOOLS_PACKAGE}" ;\
   rm -rf "${CLIENTTOOLS_PACKAGE}"


ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["sleep.sh"]
