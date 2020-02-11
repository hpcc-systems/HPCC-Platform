ARG BUILD_VER
FROM hpccsystems/platform-build:${BUILD_VER} as build
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
  
COPY --from=build /hpcc-dev/build/*.deb /
RUN dpkg -i *.deb ; \
    apt-get install -f && \
    apt-get clean && \
    rm *.deb

USER hpcc
ENV PATH="/opt/HPCCSystems/bin:${PATH}"
ENV HPCC_containerized=1
