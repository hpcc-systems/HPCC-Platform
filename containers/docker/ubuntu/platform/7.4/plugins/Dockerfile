FROM hpccsystems/platform:<tag>

#RUN apt-get update && apt-get install -y iputils-ping curl xterm
#RUN groupadd hpcc && useradd -s /bin/bash -r -m -d /home/hpcc -g hpcc -c "hpcc Runtime User" hpcc

ENV VERSION <version>
ENV FULL_VERSION <full version>
#ENV PLATFORM_DOWNLOAD_MD5  ef58e94dcc66a45cb0fc8202e68b50d9
ENV PLATFORM_PACKAGE hpccsystems-platform-community_${FULL_VERSION}bionic_amd64.deb

RUN set -x \
   &&  wget  "http://d2wulyp08c6njk.cloudfront.net/releases/CE-Candidate-${VERSION}/bin/platform/${PLATFORM_PACKAGE}" \
   #&& echo "$PLATFORM_DOWNLOAD_MD5 ${PLATFORM_PACKAGE}" | md5sum -c - \
   && dpkg -i "${PLATFORM_PACKAGE}" \
   && rm -rf "${PLATFORM_PACKAGE}"

EXPOSE 8010 8002 8015 9876

RUN cp -r /home/hpcc/.ssh ~/

ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["/usr/sbin/sshd", "-D"]
