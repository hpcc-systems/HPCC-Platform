ARG BUILD_VER
FROM hpccsystems/platform-core:${BUILD_VER}

USER hpcc
RUN mkdir -p /var/lib/HPCCSystems/eclagent
WORKDIR /var/lib/HPCCSystems/eclagent
RUN agentexec --init > eclagent.json
ENTRYPOINT ["agentexec", "--config=eclagent.json"]
