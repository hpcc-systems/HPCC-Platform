ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-centos-8:$VCPKG_REF

RUN yum remove -y java-1.* && yum install -y \
    java-11-openjdk-devel \
    python3-devel \
    epel-release
RUN yum install -y \
    R-core-devel \
    R-Rcpp-devel \
    R-RInside-devel

RUN dnf -y install gcc-toolset-11-gcc gcc-toolset-11-gcc-c++

WORKDIR /hpcc-dev

ENTRYPOINT ["/bin/bash", "--login", "-c"]
