FROM tgagor/centos-stream:stream8 AS BASE_OS

ENV VCPKG_BINARY_SOURCES="clear;nuget,GitHub,readwrite"
ENV VCPKG_NUGET_REPOSITORY=https://github.com/hpcc-systems/vcpkg

# Build Tools - Mono  ---
RUN yum update -y
RUN yum install -y yum-utils
RUN yum-config-manager --add-repo http://download.mono-project.com/repo/centos/
RUN yum clean all
RUN yum makecache
RUN rpm --import "http://keyserver.ubuntu.com/pks/lookup?op=get&search=0x3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF"
RUN dnf config-manager --add-repo https://download.mono-project.com/repo/centos8-stable.repo
RUN dnf install -y mono-complete 

# Build Tools  ---
RUN yum install -y git curl zip unzip tar python3 libtool autoconf automake
RUN yum group install -y "Development Tools"
RUN dnf -y install gcc-toolset-9-gcc gcc-toolset-9-gcc-c++

RUN echo "source /opt/rh/gcc-toolset-9/enable" >> /etc/bashrc
SHELL ["/bin/bash", "--login", "-c"]

RUN . <(curl https://aka.ms/vcpkg-init.sh -L)
RUN . ~/.vcpkg/vcpkg-init fetch cmake
RUN ln -s `. ~/.vcpkg/vcpkg-init fetch cmake | tail -n 1` /usr/local/bin/
RUN ln -s `. ~/.vcpkg/vcpkg-init fetch nuget | tail -n 1` /usr/local/bin/
RUN ln -s `. ~/.vcpkg/vcpkg-init fetch node | tail -n 1` /usr/local/bin/
RUN ln -s `. ~/.vcpkg/vcpkg-init fetch node | tail -n 1 | sed -e 's!bin/node!bin/npm!g'` /usr/local/bin/

# Libraries  ---
RUN yum remove -y java-1.8*
RUN yum install -y \
    curl-devel \
    python3-devel \
    ncurses-devel \
    openldap-devel \
    numactl-devel \
    java-11-openjdk-devel \
    sqlite-devel \
    libevent-devel

RUN dnf --enablerepo=powertools -y install libmemcached-devel

ARG GITHUB_ACTOR=hpcc-systems
ARG GITHUB_TOKEN=none
RUN mono `. ~/.vcpkg/vcpkg-init fetch nuget | tail -n 1` \
    sources add \
    -name "GitHub" \
    -source "https://nuget.pkg.github.com/hpcc-systems/index.json" \
    -storepasswordincleartext \
    -username "${GITHUB_ACTOR}" \
    -password "${GITHUB_TOKEN}"
RUN mono `. ~/.vcpkg/vcpkg-init fetch nuget | tail -n 1` \
    setapikey "${GITHUB_TOKEN}" \
    -source "https://nuget.pkg.github.com/hpcc-systems/index.json"
