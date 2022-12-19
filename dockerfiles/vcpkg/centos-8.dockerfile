ARG VCPKG_REF=latest
FROM hpccbuilds/vcpkg-centos-8:$VCPKG_REF

RUN yum remove -y java-1.* && yum install -y \
    java-11-openjdk-devel \
    python3-devel 

WORKDIR /hpcc-dev

ENTRYPOINT ["/bin/bash", "--login", "-c"]
