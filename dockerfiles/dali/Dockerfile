ARG BUILD_VER
FROM hpccsystems/platform-core:${BUILD_VER}

USER hpcc

WORKDIR /etc/HPCCSystems/
RUN sed -i "s+</DaliServerProcess>+<WhiteList enabled='false'/></DaliServerProcess>+" environment.xml
RUN mkdir -p /var/lib/HPCCSystems/dali
WORKDIR /var/lib/HPCCSystems/dali
COPY --chown=hpcc:hpcc daliconf.xml /var/lib/HPCCSystems/dali
#RUN daserver --init
ENTRYPOINT ["daserver"]

