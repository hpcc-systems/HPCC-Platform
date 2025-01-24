ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-centos-7:$VCPKG_REF

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]
