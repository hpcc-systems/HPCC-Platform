ARG VCPKG_REF=latest
FROM hpccbuilds/vcpkg-ubuntu-18.04:$VCPKG_REF

RUN apt-get update && apt-get install --no-install-recommends -y \
    default-jdk \
    python3-dev 

WORKDIR /hpcc-dev

ENV OS=ubuntu-18.04
ENV SOURCE_FOLDER=/hpcc-dev/HPCC-Platform
ENV BUILD_FOLDER=$SOURCE_FOLDER/build-$OS
ENV CMAKE_OPTIONS="\
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCONTAINERIZED=ON \
    -DUSE_OPTIONAL=OFF \
    -DINCLUDE_PLUGINS=ON \
    -DSUPPRESS_COUCHBASEEMBED=ON \
    -DSUPPRESS_ECLBLAS=ON \
    -DSUPPRESS_JAVAEMBED=ON \
    -DSUPPRESS_KAFKA=ON \
    -DSUPPRESS_MEMCACHED=ON \
    -DSUPPRESS_MYSQLEMBED=ON \
    -DSUPPRESS_REDIS=ON \
    -DSUPPRESS_REMBED=ON \
    -DSUPPRESS_SPARK=ON \
    -DSUPPRESS_SQLITE3EMBED=ON \
    -DSUPPRESS_SQS=ON \
    -DSUPPRESS_V8EMBED=ON \
    -DUSE_AWS=OFF \
    -DUSE_JAVA=OFF \
    -DUSE_PYTHON3=OFF \
    -DCPACK_THREADS=0 \
    -DCPACK_STRIP_FILES=ON \
    "

ENTRYPOINT ["/bin/bash", "--login", "-c", \
    "mkdir -p ${BUILD_FOLDER} && \
    cp -R /hpcc-dev/build/* $BUILD_FOLDER && \
    cmake -S ${SOURCE_FOLDER} -B ${BUILD_FOLDER} ${CMAKE_OPTIONS} && \
    cmake --build ${BUILD_FOLDER} --target package -- -j" \
    ]