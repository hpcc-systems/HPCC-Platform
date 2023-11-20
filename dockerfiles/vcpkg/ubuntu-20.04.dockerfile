ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-ubuntu-20.04:$VCPKG_REF

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]