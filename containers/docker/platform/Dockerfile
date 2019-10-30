FROM hpccsystems/hpcc-base:7-bionic

ARG version=
RUN if [ -z "${version}" ] ; then echo Missing HPCC version; exit 1; else : ; fi

RUN set -ex; \
   apt-get update; \
   apt-get install -y iputils-ping curl xterm libarchive13 gpg ; \
   groupadd hpcc && useradd -s /bin/bash -r -m -d /home/hpcc -g hpcc -c "hpcc Runtime User" hpcc ; \
   export VERSION_MMP=${version%-*}; \
   export PLATFORM_PACKAGE=hpccsystems-platform-community_${version}bionic_amd64.deb; \
   wget  "http://d2wulyp08c6njk.cloudfront.net/releases/CE-Candidate-${VERSION_MMP}/bin/platform/${PLATFORM_PACKAGE}"; \
   #echo "$PLATFORM_DOWNLOAD_MD5 ${PLATFORM_PACKAGE}" | md5sum -c -; \
   dpkg -i "${PLATFORM_PACKAGE}" ;\
   rm -rf "${PLATFORM_PACKAGE}"


EXPOSE 8010 8002 8015 9876 18010 18002 18015 19876

#COPY id_rsa* /home/hpcc/.ssh/

#RUN cp -r /home/hpcc/.ssh ~/

ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["/usr/sbin/sshd", "-D"]
