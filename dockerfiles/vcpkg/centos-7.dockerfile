ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-centos-7:$VCPKG_REF

RUN yum makecache && yum install -y \
    epel-release \
    java-11-openjdk-devel \
    python3-devel \
    wget && \
    yum update -y && yum install -y \
    ccache \
    R-core-devel && \
    yum -y clean all && rm -rf /var/cache

ENV Rcpp_package=Rcpp_0.12.19.tar.gz
ENV RInside_package=RInside_0.2.12.tar.gz

RUN wget https://cran.r-project.org/src/contrib/Archive/Rcpp/${Rcpp_package}
RUN wget https://cran.r-project.org/src/contrib/Archive/RInside/${RInside_package}
RUN R CMD INSTALL ${Rcpp_package} ${RInside_package}
RUN rm -f ${Rcpp_package} ${RInside_package}

WORKDIR /hpcc-dev

ENTRYPOINT ["/bin/bash", "--login", "-c"]
