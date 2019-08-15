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
