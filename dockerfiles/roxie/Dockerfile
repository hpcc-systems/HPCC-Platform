ARG BUILD_VER
FROM hpccsystems/platform-core:${BUILD_VER}

USER hpcc
RUN mkdir -p /var/lib/HPCCSystems/roxie
WORKDIR /var/lib/HPCCSystems/roxie
RUN roxie --init > roxie.json
ENTRYPOINT ["roxie", "--config=roxie.json"]

