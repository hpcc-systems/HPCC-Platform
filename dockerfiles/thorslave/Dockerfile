ARG BUILD_VER
FROM hpccsystems/platform-core:${BUILD_VER}

USER hpcc
RUN mkdir -p /var/lib/HPCCSystems/thor
WORKDIR /var/lib/HPCCSystems/thor
ENTRYPOINT ["thorslave_lcr"]

