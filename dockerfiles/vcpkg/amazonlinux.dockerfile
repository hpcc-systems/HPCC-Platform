ARG VCPKG_REF=latest
FROM hpccbuilds/vcpkg-amazonlinux:$VCPKG_REF

RUN yum install -y \
    java-11-openjdk-devel \
    python3-devel 

WORKDIR /hpcc-dev

ENTRYPOINT ["/bin/bash", "--login", "-c"]
