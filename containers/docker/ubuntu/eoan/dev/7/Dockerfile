From ubuntu:19.10

# fails: curl -sL https://deb.nodesource.com/setup_12.x | bash - ; \
## Your distribution, identified as "Ubuntu Eoan Ermine (development branch)", is a pre-release version of Ubuntu. NodeSource does not maintain official support for Ubuntu versions until they are formally released. You can try using the manual installation instructions available at https://github.com/nodesource/distributions and use the latest supported Ubuntu version name as the distribution identifier, although this is not guaranteed to work.

RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        wget                  \
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
        g++                   \
        gcc                   \
        curl                  \
        libfreetype6-dev      \
        make                  \
        git                   \
        libicu-dev            \
        libxslt1-dev          \
        zlib1g-dev            \
        libevent-dev          \
        libsqlite3-dev        \
        libtool               \
        autotools-dev         \
        automake              \
        m4                    \
        libssl-dev            \
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
#RUN set -ex; \
#  wget http://10.240.32.242/data3/build/gpg/HPCCSystems.priv
#  #aws s3 cp s3://@S3_BUCKET@/HPCCSystems.priv .
#  gpg --passphrase @GPG_PASSPHRASE@  --import HPCCSystems.priv
#  rm -rf HPCCSystems.priv

# Documenttion build requisites
# libsaxonb-java
# fop
# xsltproc
