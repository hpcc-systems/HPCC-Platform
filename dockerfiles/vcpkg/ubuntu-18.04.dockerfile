ARG VCPKG_REF=latest
ARG DOCKER_NAMESPACE=hpccbuilds
FROM ${DOCKER_NAMESPACE}/vcpkg-ubuntu-18.04:$VCPKG_REF

RUN apt-get update && apt-get install --no-install-recommends -y \
    default-jdk \
    python3-dev \
    r-base \
    r-cran-rcpp \
    r-cran-rinside \
    r-cran-inline

WORKDIR /hpcc-dev

ENTRYPOINT ["/bin/bash", "--login", "-c"]
