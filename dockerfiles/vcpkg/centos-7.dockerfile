ARG VCPKG_REF=latest
FROM hpccbuilds/vcpkg-centos-7:$VCPKG_REF

RUN yum install -y \
    java-11-openjdk-devel \
    python3-devel 

WORKDIR /hpcc-dev

ENTRYPOINT ["/bin/bash", "--login", "-c"]
