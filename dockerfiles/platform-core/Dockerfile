ARG BUILD_LABEL
FROM hpccsystems/platform-build:${BUILD_LABEL} as build
FROM ubuntu:18.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt clean && \
    apt autoclean && \
    apt install -f && \
    apt autoremove && \
    apt-get update

RUN apt-get install -y \
  xterm \
  g++ \
  openssh-client \
  openssh-server \
  expect \
  rsync \
  libapr1 \
  python \ 
  psmisc \
  curl
  
RUN apt-get install -y \
  libnuma1 \
  libboost-regex1.65.1 \
  libpython3.6 \
  libmemcached11 \
  libmemcachedutil2

RUN apt-get install -y \
  libxslt1.1

RUN apt-get install -y \
    dnsutils \
    nano 

COPY --from=build /opt/HPCCSystems /

#RUN dpkg -i *.deb ; \
#    apt-get install -f && \
#    apt-get clean && \
#    rm *.deb

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
