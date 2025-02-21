ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-ubuntu-22.04:hpcc-platform-9.10.x-arm

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]
