ARG VCPKG_REF=latest
FROM hpccbuilds/vcpkg-ubuntu-22.10:$VCPKG_REF

RUN apt-get update && apt-get install --no-install-recommends -y \
    default-jdk \
    python3-dev

WORKDIR /hpcc-dev

ENTRYPOINT ["/bin/bash", "--login", "-c"]
