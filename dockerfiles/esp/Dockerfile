ARG BUILD_VER
FROM hpccsystems/platform-core:${BUILD_VER}

USER hpcc
RUN mkdir -p /var/lib/HPCCSystems/esp
WORKDIR /var/lib/HPCCSystems/esp
COPY --chown=hpcc:hpcc esp.xml /var/lib/HPCCSystems/esp
#RUN esp --init
ENTRYPOINT ["esp"]

