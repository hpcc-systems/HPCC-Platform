ARG BUILD_VER
FROM hpccsystems/platform-core:${BUILD_VER}

USER hpcc
RUN mkdir -p /var/lib/HPCCSystems/thor
WORKDIR /var/lib/HPCCSystems/thor
COPY --chown=hpcc:hpcc thor.xml /var/lib/HPCCSystems/thor
#RUN thormaster_lcr --init
ENTRYPOINT ["thormaster_lcr"]

