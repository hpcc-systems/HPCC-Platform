ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-rockylinux-8:$VCPKG_REF

ENTRYPOINT ["/bin/bash", "--login", "-c"]

RUN yum install -y \
    rpm-build && \
    yum -y clean all && rm -rf /var/cache 

CMD ["/bin/bash"]
