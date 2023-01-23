ARG VCPKG_REF=latest
ARG DOCKER_NAMESPACE=hpccbuilds
FROM ${DOCKER_NAMESPACE}/vcpkg-ubuntu-20.04:$VCPKG_REF

ENV RInside_package=RInside_0.2.14.tar.gz

RUN apt-get update && apt-get install --no-install-recommends -y \
    default-jdk \
    python3-dev \
    wget \
    r-base \
    r-cran-rcpp
RUN wget https://cran.r-project.org/src/contrib/Archive/RInside/${RInside_package}
RUN R CMD INSTALL ${RInside_package}
RUN rm -f ${RInside_package}

WORKDIR /hpcc-dev

ENTRYPOINT ["/bin/bash", "--login", "-c"]
