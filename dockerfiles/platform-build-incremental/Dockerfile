ARG BUILD_VER
FROM hpccsystems/platform-build:${BUILD_VER}

ARG USER=richardkchapman
WORKDIR /hpcc-dev/HPCC-Platform
RUN git remote add user https://github.com/${USER}/HPCC-Platform.git

ARG COMMIT

RUN git fetch user && git checkout ${COMMIT} 
WORKDIR /hpcc-dev/build
RUN rm *.deb && make -j$(($(nproc)*3/2)) package
