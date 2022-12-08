ARG VCPKG_REF=latest
FROM hpccbuilds/vcpkg-centos-7:$VCPKG_REF

RUN yum install -y \
    java-11-openjdk-devel \
    python3-devel 

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
