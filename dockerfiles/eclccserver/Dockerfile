ARG BUILD_LABEL
FROM hpccsystems/platform-core:${BUILD_LABEL}

USER hpcc
RUN mkdir -p /var/lib/HPCCSystems/eclccserver
WORKDIR /var/lib/HPCCSystems/eclccserver
RUN eclccserver --init > eclccserver.json
ENTRYPOINT [ "eclccserver", "--config=eclccserver.json" ]
