# A version of the platform-core Dockerfile that keeps all the build artefacts in place, for use by developers wanting to debug the platform

ARG BUILD_LABEL
FROM hpccsystems/platform-build:${BUILD_LABEL}

RUN apt-get install -y \
    dnsutils \
    nano 

RUN dpkg -i /hpcc-dev/build/*.deb ; \
    apt-get install -f -y

USER hpcc
ENV PATH="/opt/HPCCSystems/bin:${PATH}"
ENV HPCC_containerized=1
ENV HPCC_DLLSERVER_PATH=/var/lib/HPCCSystems/queries
