ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-amazonlinux:$VCPKG_REF

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]
