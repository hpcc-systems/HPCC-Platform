ARG BUILD_LABEL
FROM hpccsystems/platform-core:${BUILD_LABEL}

USER hpcc
RUN mkdir -p /var/lib/HPCCSystems/toposerver
WORKDIR /var/lib/HPCCSystems/toposerver
RUN toposerver --init > toposerver.json
ENTRYPOINT ["toposerver", "--config=toposerver.json"]

