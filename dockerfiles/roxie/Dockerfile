ARG BUILD_LABEL
FROM hpccsystems/platform-core:${BUILD_LABEL}

USER hpcc
RUN mkdir -p /var/lib/HPCCSystems/roxie
WORKDIR /var/lib/HPCCSystems/roxie
RUN roxie --init > roxie.json
ENTRYPOINT ["roxie", "--config=roxie.json"]

