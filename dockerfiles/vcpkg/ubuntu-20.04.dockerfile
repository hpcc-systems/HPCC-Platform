ARG VCPKG_REF=latest
FROM hpccbuilds/vcpkg-ubuntu-20.04:$VCPKG_REF

RUN apt-get update && apt-get install --no-install-recommends -y \
    default-jdk \
    python3-dev

WORKDIR /hpcc-dev

ENV CMAKE_OPTIONS="\
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DVCPKG_FILES_DIR=/hpcc-dev \
    -DCONTAINERIZED=ON \
    -DUSE_OPTIONAL=OFF \
    -DINCLUDE_PLUGINS=ON \
    -DSUPPRESS_REMBED=ON \
    -DSUPPRESS_V8EMBED=ON \
    "

ENTRYPOINT ["/bin/bash", "--login", "-c"]
