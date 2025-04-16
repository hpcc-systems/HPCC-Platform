ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-centos-7:$VCPKG_REF

RUN yum install -y flex bison rpm-build && \
    yum clean all 

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]
