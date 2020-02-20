ARG PREV_LABEL
FROM hpccsystems/platform-build:${PREV_LABEL}

ARG USER=richardkchapman
WORKDIR /hpcc-dev/HPCC-Platform
RUN if ! git config remote.${USER}.url > /dev/null ; then git remote add ${USER} https://github.com/${USER}/HPCC-Platform.git ; fi

ARG COMMIT

RUN git fetch ${USER} && git checkout ${COMMIT} 
WORKDIR /hpcc-dev/build
RUN rm *.deb && make -j$(($(nproc)*3/2)) package
