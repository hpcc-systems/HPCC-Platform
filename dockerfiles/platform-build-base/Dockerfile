FROM ubuntu:18.04 as build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt clean && \
    apt autoclean && \
    apt install -f && \
    apt autoremove && \
    apt-get update

RUN apt-get install -y \
  automake \
  autotools-dev \
  binutils-dev \
  bison \
  build-essential \
  curl \
  default-jdk \
  default-libmysqlclient-dev \
  flex \
  libapr1-dev \
  libaprutil1-dev \
  libarchive-dev \
  libatlas-base-dev \
  libblas-dev \
  libboost-regex-dev \
  libcppunit-dev \
  libcurl4-openssl-dev \
  libevent-dev \
  libhiredis-dev \
  libiberty-dev \
  libicu-dev \
  libldap2-dev \
  libmemcached-dev \
  libnuma-dev \
  libsqlite3-dev \
  libssl-dev \
  libtbb-dev \
  libtool \
  libv8-dev \
  libxalan-c-dev \
  libxslt1-dev \
  pkg-config \
  python-dev \
  python3-dev \
  r-base-dev \
  r-cran-inline \
  r-cran-rcpp \
  r-cran-rinside \
  rsync \
  psmisc \
  libpopt0 \
  zlib1g-dev

RUN apt-get install -y \
  bash-completion \
  expect \
  git \
  nano \
  valgrind \
  sudo \
  vim \
  gdb \
  software-properties-common \  
  lsb-release

RUN curl https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add -
RUN apt-add-repository "deb https://deb.nodesource.com/node_10.x $(lsb_release -sc) main"
RUN apt-get update
RUN apt-get install nodejs

RUN mkdir /home/temp
WORKDIR /home/temp
RUN curl https://cmake.org/files/v3.16/cmake-3.16.3.tar.gz | tar xvz
WORKDIR /home/temp/cmake-3.16.3/
RUN ./bootstrap
RUN make -j4
RUN make install
WORKDIR /
RUN rm -rf /home/temp
RUN apt-get clean

FROM ubuntu:18.04
COPY --from=build / /
