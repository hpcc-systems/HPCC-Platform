ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-rockylinux-8:$VCPKG_REF

RUN yum install -y \
    rpm-build && \
    yum -y clean all && rm -rf /var/cache 

ARG GROUP_ID=10001
ARG USER_ID=10000
RUN groupadd --gid $GROUP_ID hpcc && \
    useradd --shell /bin/bash --comment "hpcc runtime User" --uid $USER_ID --gid $GROUP_ID hpcc

RUN chown hpcc:hpcc /hpcc-dev && \
    chown hpcc:hpcc /hpcc-dev/vcpkg_installed && \
    chown -R hpcc:hpcc /hpcc-dev/vcpkg_installed/vcpkg

USER hpcc

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]
