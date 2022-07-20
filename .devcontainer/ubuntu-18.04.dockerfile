
FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /workspaces/HPCC-Platform

RUN apt update
RUN apt install -y git curl zip unzip tar bison flex
RUN apt install -y build-essential
RUN apt install -y pkg-config libtool autotools-dev automake
RUN apt install -y libldap2-dev libnuma-dev libcurl4-openssl-dev uuid-dev libssl-dev libncurses-dev
RUN apt install -y libmemcached-dev libmysqlclient-dev libsqlite3-dev
RUN apt install -y default-jdk python3-dev

WORKDIR /workspaces
RUN git clone -n https://github.com/Kitware/CMake.git
WORKDIR /workspaces/CMake
RUN git checkout v3.23.2
RUN ./bootstrap && make -j && make install

RUN apt install -y dirmngr gnupg apt-transport-https ca-certificates
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF
RUN sh -c 'echo "deb https://download.mono-project.com/repo/ubuntu stable-bionic main" > /etc/apt/sources.list.d/mono-official-stable.list'

RUN apt install -y gnupg lsb-release software-properties-common
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash -

RUN apt update
RUN apt install -y nodejs mono-complete

WORKDIR /workspaces/HPCC-Platform

ENV VCPKG_BINARY_SOURCES="clear;files,/workspaces/HPCC-Platform/.devcontainer/vcpkg-cache,readwrite"

ARG GITHUB_ACTOR=hpcc-systems
ARG GITHUB_TOKEN=none

. <(curl https://aka.ms/vcpkg-init.sh -L)
https://github.com/microsoft/vcpkg-tool