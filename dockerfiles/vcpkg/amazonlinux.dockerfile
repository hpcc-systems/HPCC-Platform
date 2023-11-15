ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-amazonlinux:$VCPKG_REF

RUN amazon-linux-extras install java-openjdk11 && yum install -y \
    java-11-openjdk-devel \
    python3-devel \
    epel-release
RUN yum install -y \
    ccache \
    R-core-devel \
    R-Rcpp-devel \
    R-RInside-devel

WORKDIR /hpcc-dev

ENTRYPOINT ["/bin/bash", "--login", "-c"]
