ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-centos-7:hpcc-platform-9.8.x

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]
