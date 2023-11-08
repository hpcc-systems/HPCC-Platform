ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-centos-8:$VCPKG_REF

RUN yum remove -y python3.11 java-1.* && yum install -y \
    java-11-openjdk-devel \
    python3-devel \
    epel-release && \
    yum update -y && yum install -y \
    ccache \
    R-core-devel \
    R-Rcpp-devel \
    R-RInside-devel

WORKDIR /hpcc-dev

ENTRYPOINT ["/bin/bash", "--login", "-c"]
