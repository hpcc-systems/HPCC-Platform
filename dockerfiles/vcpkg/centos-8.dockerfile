ARG VCPKG_REF=latest
FROM hpccbuilds/vcpkg-centos-8:$VCPKG_REF

RUN yum remove -y java-1.* && yum install -y \
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
