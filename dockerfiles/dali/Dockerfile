ARG BUILD_LABEL
FROM hpccsystems/platform-core:${BUILD_LABEL}

USER hpcc

RUN mkdir -p /var/lib/HPCCSystems/dali
WORKDIR /var/lib/HPCCSystems/dali
COPY --chown=hpcc:hpcc daliconf.xml /var/lib/HPCCSystems/dali
COPY --chown=hpcc:hpcc environment.xml /etc/HPCCSystems/environment.xml
COPY --chown=hpcc:hpcc environment.conf /etc/HPCCSystems/environment.conf
#RUN daserver --init
ENTRYPOINT ["daserver"]

