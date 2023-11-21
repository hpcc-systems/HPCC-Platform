ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-ubuntu-23.10:$VCPKG_REF

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]