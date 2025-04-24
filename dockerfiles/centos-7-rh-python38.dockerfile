ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-centos-7:$VCPKG_REF

RUN yum remove -y python3 python3-devel && \
    yum install -y flex bison rh-python38 rh-python38-python-devel rpm-build && \
    yum clean all

RUN echo "source /opt/rh/rh-python38/enable" >> /etc/bashrc
SHELL ["/bin/bash", "--login", "-c"]

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]
