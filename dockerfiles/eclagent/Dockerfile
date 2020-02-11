ARG BUILD_VER
FROM hpccsystems/platform-core:${BUILD_VER}

USER hpcc
RUN mkdir -p /var/lib/HPCCSystems/eclagent
WORKDIR /var/lib/HPCCSystems/eclagent
COPY --chown=hpcc:hpcc agentexec.xml /var/lib/HPCCSystems/eclagent
#RUN roxie --init
ENTRYPOINT ["agentexec"]

