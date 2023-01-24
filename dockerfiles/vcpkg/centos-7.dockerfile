ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-centos-7:$VCPKG_REF

RUN yum install -y \
    java-11-openjdk-devel \
    python3-devel \
    wget \
    epel-release
RUN yum update -y && yum install -y R-core-devel

ENV Rcpp_package=Rcpp_0.12.19.tar.gz
ENV RInside_package=RInside_0.2.12.tar.gz

RUN wget https://cran.r-project.org/src/contrib/Archive/Rcpp/${Rcpp_package}
RUN wget https://cran.r-project.org/src/contrib/Archive/RInside/${RInside_package}
RUN R CMD INSTALL ${Rcpp_package} ${RInside_package}
RUN rm -f ${Rcpp_package} ${RInside_package}

WORKDIR /hpcc-dev

ENTRYPOINT ["/bin/bash", "--login", "-c"]
