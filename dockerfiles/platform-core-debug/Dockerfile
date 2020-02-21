# A version of the platform-core Dockerfile that keeps all the build artefacts in place, for use by developers wanting to debug the platform

ARG BUILD_LABEL
FROM hpccsystems/platform-build:${BUILD_LABEL}

RUN apt-get install -y \
    dnsutils \
    nano 

RUN useradd -s /bin/bash -r -U -c "hpcc runtime User" hpcc
RUN passwd -l hpcc 

RUN mkdir /var/lib/HPCCSystems && chown hpcc:hpcc /var/lib/HPCCSystems
RUN mkdir /var/log/HPCCSystems && chown hpcc:hpcc /var/log/HPCCSystems
RUN mkdir /var/lock/HPCCSystems && chown hpcc:hpcc /var/lock/HPCCSystems
RUN mkdir /var/run/HPCCSystems && chown hpcc:hpcc /var/run/HPCCSystems

USER hpcc

ENV PATH="/opt/HPCCSystems/bin:${PATH}"
ENV HPCC_containerized=1
ENV HPCC_DLLSERVER_PATH=/var/lib/HPCCSystems/queries
