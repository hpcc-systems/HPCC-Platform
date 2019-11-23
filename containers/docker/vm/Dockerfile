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



COPY start-hpcc /usr/local/bin/
ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["/usr/sbin/sshd", "-D"]
