ARG BUILD_VER
FROM hpccsystems/platform-build:${BUILD_VER}

ARG BUILD_USER=richardkchapman
WORKDIR /hpcc-dev/HPCC-Platform
RUN bash -c "[[ -z \"$(git remote | fgrep ${BUILD_USER})\" ]] && git remote add ${BUILD_USER} https://github.com/${BUILD_USER}/HPCC-Platform.git" || true
#RUN [[ -z "$(git remote | fgrep ${BUILD_USER})" ]] && git remote add ${BUILD_USER} https://github.com/${BUILD_USER}/HPCC-Platform.git

ARG COMMIT

RUN git fetch ${BUILD_USER} && git checkout ${COMMIT} 
WORKDIR /hpcc-dev/build
RUN rm *.deb && make -j$(($(nproc)*3/2)) package
