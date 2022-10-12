FROM ubuntu:22.04 AS base_build

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y

WORKDIR /hpcc-dev

COPY ./*.deb .

RUN apt-get install --no-install-recommends -f -y ./*.deb

EXPOSE 8010

ENTRYPOINT ["touch /var/log/HPCCSystems/myesp/esp.log && \
    /etc/init.d/hpcc-init start && \
    tail -F /var/log/HPCCSystems/myesp/esp.log" \
    ]
