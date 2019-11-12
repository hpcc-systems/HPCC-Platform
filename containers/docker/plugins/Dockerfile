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
FROM hpccsystems/platform

#RUN apt-get update && apt-get install -y iputils-ping curl xterm
#RUN groupadd hpcc && useradd -s /bin/bash -r -m -d /home/hpcc -g hpcc -c "hpcc Runtime User" hpcc

ARG version=
RUN if [ -z "${version}" ] ; then echo Missing HPCC version; exit 1; else : ; fi


RUN set -x \
   apt-get update; \
   apt-get install -y default-jdk libmysqlclient20 libcurl3-gnutls ; \
   version_mmp=$(echo $version | cut -d'-' -f1) ; \
   urlbase="http://d2wulyp08c6njk.cloudfront.net/releases/CE-Candidate-${version_mmp}/bin/plugins" ; \
   plugins="couchbaseembed javaembed kafka memcached mysqlembed redis sqlite3embed sqs"; \
   for plugin in $plugins ;\
   do \
      plugin_file="hpccsystems-plugin-${plugin}_${version}bionic_amd64.deb"; \
      url="${urlbase}/${plugin_file}"; \
      wget $url ; \
      if [ $? -ne 0 ] ; \
      then  \
         echo "Failed to get $url" ; \
         exit 1; \
      else : ; fi ; \
      dpkg -i "${plugin_file}"; \
      rm -rf "${plugin_file}"; \
   done



ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["/usr/sbin/sshd", "-D"]
