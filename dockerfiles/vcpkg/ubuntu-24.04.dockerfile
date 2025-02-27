ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-ubuntu-24.04:$VCPKG_REF

ARG GROUP_ID=10001
ARG USER_ID=10000
RUN addgroup --gid $GROUP_ID hpcc && \
    adduser --disabled-password --gecos "hpcc runtime User" --uid $USER_ID --gid $GROUP_ID hpcc

RUN chown hpcc:hpcc /hpcc-dev && \
    chown hpcc:hpcc /hpcc-dev/vcpkg_installed && \
    chown -R hpcc:hpcc /hpcc-dev/vcpkg_installed/vcpkg

USER hpcc

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]