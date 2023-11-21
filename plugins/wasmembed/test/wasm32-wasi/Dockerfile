FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y \
    autoconf \
    autogen \
    automake \
    clang \
    cmake \
    curl \
    libtool \
    lld \
    llvm \
    make \
    ninja-build \
    wget 

RUN curl --proto '=https' --tlsv1.3 https://sh.rustup.rs -sSf | sh -s -- -y

SHELL ["/bin/bash", "--login", "-c"]

WORKDIR /hpcc-dev

ARG WIT_VERSION=0.9.0
RUN cargo install wasm-tools && \
    cargo install --git https://github.com/bytecodealliance/wit-bindgen --tag wit-bindgen-cli-${WIT_VERSION} wit-bindgen-cli && \
    curl https://wasmtime.dev/install.sh -sSf | bash 

# List of current vertsion can be found in https://github.com/bytecodealliance/wit-bindgen/releases  ---
ARG WASI_VERSION=20
ARG WASI_MINOR_VERSION=0
ARG WASI_VERSION_FULL=${WASI_VERSION}.${WASI_MINOR_VERSION}
RUN wget https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_VERSION}/wasi-sdk-${WASI_VERSION_FULL}-linux.tar.gz
RUN tar xvf wasi-sdk-${WASI_VERSION_FULL}-linux.tar.gz && rm wasi-sdk-${WASI_VERSION_FULL}-linux.tar.gz
RUN mv wasi-sdk-${WASI_VERSION_FULL} wasi-sdk

WORKDIR /hpcc-dev/wasmembed

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["bash"]
