ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-ubuntu-23.04:$VCPKG_REF

RUN apt-get update && apt-get install --no-install-recommends -y \
    default-jdk \
    ninja-build \
    python3-dev \
    rsync \
    fop \
    libsaxonb-java \
    r-base \
    r-cran-rcpp \
    r-cran-rinside \
    r-cran-inline

RUN git config --global --add safe.directory '*'

WORKDIR /hpcc-dev

ENTRYPOINT ["/bin/bash", "--login", "-c"]
