ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-amazonlinux:$VCPKG_REF

RUN yum install -y rpm-build && \
    yum clean all

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]
