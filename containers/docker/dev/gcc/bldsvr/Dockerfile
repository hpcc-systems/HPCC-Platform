################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################
From gcc:9

RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        zip                   \
        build-essential       \
        binutils-dev          \
        libldap2-dev          \
        libcppunit-dev        \
        libboost-regex-dev    \
        libarchive-dev        \
        libv8-dev             \
        libapr1-dev           \
        libaprutil1-dev       \
        libiberty-dev         \
        libhiredis-dev        \
        libtbb-dev            \
        libxalan-c-dev        \
        libnuma-dev           \
        libmemcached-dev      \
        libboost-thread-dev   \
        libboost-filesystem-dev \
        bison                 \
        flex                  \
        libatlas-base-dev     \
        python2.7-dev         \
        python3-dev           \
        libgtk2.0-dev         \
        libcurl4-gnutls-dev   \
        default-jdk           \
# gcc already has following packages
        #g++                   \
        #gcc                   \
        #curl                  \
        #libfreetype6-dev      \
        #make                  \
        #git                   \
        #libicu-dev            \
        #libxslt1-dev          \
        #zlib1g-dev            \
        #libevent-dev          \
        #libsqlite3-dev        \
        #libtool               \
        #autotools-dev         \
        #automake              \
        #m4                    \
        #libssl-dev            \
        --fix-missing;        \
    \
    curl -sL https://deb.nodesource.com/setup_12.x | bash - ; \
    apt-get install -y nodejs; \
    \
    export cmake_version=3.15.2; \
    export cmake_name=cmake-${cmake_version}-Linux-x86_64; \
    wget https://github.com/Kitware/CMake/releases/download/v${cmake_version}/${cmake_name}.tar.gz; \
    tar -zxf ${cmake_name}.tar.gz; \
    rm -rf ${cmake_name}.tar.gz; \
    cp -r ${cmake_name}/bin ${cmake_name}/share /usr/local/ ; \
    rm -rf ${cmake_name}

# Import GPG key
RUN set -ex; \
  wget http://10.240.32.242/data3/build/gpg/HPCCSystems.priv; \
  #aws s3 cp s3://@S3_BUCKET@/HPCCSystems.priv . ; \
  #gpg --pinentry-mode=loopback --passphrase @GPG_PASSPHRASE@  --import HPCCSystems.priv; \
  gpg --pinentry-mode=loopback --passphrase icanspellthis  --import HPCCSystems.priv; \
  rm -rf HPCCSystems.priv

# Documenttion build requisites
# libsaxonb-java
# fop
# xsltproc
