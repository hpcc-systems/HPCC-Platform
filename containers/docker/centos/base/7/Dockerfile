FROM centos:7

RUN set -ex; \
    yum update -y && yum install -y \
                epel-release          \
                wget                  \
                gcc-c++               \
                boost-regex           \
                openldap              \
                libicu                \
                m4                    \
                libtool               \
                libxslt               \
                libxml2               \
                openssh-server        \
                openssh-clients       \
                expect                \
                libarchive            \
                rsync                 \
                apr                   \
                apr-util              \
                zip                   \
                tbb                   \
                net-tools             \
                sudo                  \
                psmisc                \
                blas                  \
                atlas                 \
                which                 \
                numactl-libs          \
                numactl-devel         \
                libmemcached          \
                vim-minimal           \
                python      \
                python-libs; \
    yum install -y python34 python34-libs

#RUN package-cleanup --leaves --all
#RUN mkdir -p /var/run/dbus

RUN /usr/bin/ssh-keygen -A &&  mkdir -p /var/run/sshd
COPY docker-entrypoint.sh /
COPY sleep.sh /

CMD ["/sleep.sh"]
