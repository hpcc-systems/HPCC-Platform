ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-ubuntu-24.04:$VCPKG_REF

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]