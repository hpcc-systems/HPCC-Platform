ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-amazonlinux:$VCPKG_REF

RUN yum install -y \
    rpm-build && \
    yum -y clean all && rm -rf /var/cache 

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]