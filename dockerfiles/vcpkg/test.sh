#!/bin/bash

mkdir build
docker build --pull --rm -f "./dockerfiles/vcpkg/configure/Dockerfile" -t vcpkg-configure:latest \
    --build-arg UID=$(id -u) \
    --build-arg GID=$(id -g) \
    "."

docker run -u $(id -u):$(id -g) --rm \
    -v ${PWD}/build:/build \
    -v ${PWD}:/src \
    vcpkg-configure:latest

docker build --pull --rm -f "./dockerfiles/vcpkg/build/Dockerfile" -t vcpkg-build:latest \
    --build-arg UID=$(id -u) \
    --build-arg GID=$(id -g) \
    "."

docker run -u $(id -u):$(id -g) --rm \
    -v ${PWD}/build:/build \
    -v ${PWD}:/src \
    vcpkg-build:latest

docker build --pull --rm -f "./dockerfiles/vcpkg/test/Dockerfile" -t vcpkg-test:latest "."

docker run --rm vcpkg-test:latest
