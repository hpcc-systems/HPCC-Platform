ARG BUILD_VER
FROM hpccsystems/platform-core:${BUILD_VER}

USER hpcc
RUN mkdir -p /var/lib/HPCCSystems/eclccserver
WORKDIR /var/lib/HPCCSystems/eclccserver
RUN eclccserver --init > eclccserver.json
ENTRYPOINT dafilesrv -d && eclccserver --config=eclccserver.json

