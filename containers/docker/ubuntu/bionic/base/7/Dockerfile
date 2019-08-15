FROM ubuntu:bionic

RUN apt-get update && apt-get install -y --no-install-recommends \
		ca-certificates       \
		wget                  \
                libaprutil1           \
                libboost-regex1.65.1  \
                libicu60              \
                libldap-2.4-2         \
                libtbb2               \
                libnuma1              \
                libxml2               \
                libxslt1.1            \
                g++                   \
                openssh-client        \
                openssh-server        \
                expect                \
                rsync                 \
                python                \
                sudo                  \
                libblas3              \
                libatlas3-base        \
                psmisc                \
                libmemcached11        \
                libmemcachedutil2     \
                libpython2.7          \
                libpython3.6          \
                vim                   \
                net-tools             \
                --fix-missing         \
	&& rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/run/dbus && mkdir -p /var/run/sshd

COPY docker-entrypoint.sh /
COPY sleep.sh /

CMD ["/sleep.sh"]
