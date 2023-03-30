ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-ubuntu-22.04:$VCPKG_REF

RUN apt-get update && apt-get install --no-install-recommends -y \
    default-jdk \
    ninja-build \
    python3-dev \
    r-base \
    r-cran-rcpp \
    r-cran-rinside \
    r-cran-inline

RUN apt-get update && apt-get install --no-install-recommends -y \
    wget \
    build-essential checkinstall zlib1g-dev libssl-dev

RUN wget https://github.com/Kitware/CMake/releases/download/v3.26.1/cmake-3.26.1-linux-aarch64.sh
RUN chmod +x ./cmake-3.26.1-linux-aarch64.sh
RUN ./cmake-3.26.1-linux-aarch64.sh --skip-license

RUN git config --global --add safe.directory '*'

RUN apt-get install --no-install-recommends -y \
    rsync

# WORKDIR /hpcc-dev/cmake-3.25.3
# RUN cmake .
# RUN make -j
# RUN make install

WORKDIR /hpcc-dev/HPCC-Platform
