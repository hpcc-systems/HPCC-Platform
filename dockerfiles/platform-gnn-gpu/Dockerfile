##############################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################

# Create base container image to be used by all HPCC processes
# MORE - some of these dependencies are probably not needed by all derived containers - perhaps we should move them
# Others may not be wanted at all in container mode - tensoflow and nvidia  example??

ARG BUILD_LABEL
ARG DOCKER_REPO
FROM ${DOCKER_REPO}/platform-core:${BUILD_LABEL}
USER root

ENV NVARCH=x86_64
ENV NVIDIA_REQUIRE_CUDA=cuda>=11.4 brand=tesla,driver>=418,driver<419 brand=tesla,driver>=450,driver<451
ENV NV_CUDA_CUDART_VERSION=11.4.108-1
ENV NV_CUDA_COMPAT_PACKAGE=cuda-compat-11-4
ARG TARGETARCH=amd64

# For libraries in the cuda-compat-* package: https://docs.nvidia.com/cuda/eula/index.html#attachment-a
# Reference https://hub.docker.com/layers/cuda/nvidia/cuda/11.4.3-cudnn8-runtime-ubuntu20.04/images/sha256-3c53124b4cdd6d6b8f4f7cef889b939ee776709a2249a0eb394d228af7623da6?context=explore
RUN  apt-get update && apt-get install -y --no-install-recommends gnupg2 wget curl ca-certificates && \
    curl -fsSL https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/${NVARCH}/3bf863cc.pub | apt-key add - && \
    echo "deb https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/${NVARCH} /" > /etc/apt/sources.list.d/cuda.list && \
    apt-get purge --autoremove -y curl  && rm -rf /var/lib/apt/lists/* # buildkit

ENV CUDA_VERSION=11.4.2
RUN apt-get update && apt-get install -y --no-install-recommends \
   cuda-cudart-11-4=${NV_CUDA_CUDART_VERSION}     ${NV_CUDA_COMPAT_PACKAGE}  \
   && ln -s cuda-11.4 /usr/local/cuda &&     rm -rf /var/lib/apt/lists/* # buildkit

RUN echo "/usr/local/nvidia/lib" >> /etc/ld.so.conf.d/nvidia.conf \
    && echo "/usr/local/nvidia/lib64" >> /etc/ld.so.conf.d/nvidia.conf # buildkit

ENV PATH=/usr/local/nvidia/bin:/usr/local/cuda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV LD_LIBRARY_PATH=/usr/local/nvidia/lib:/usr/local/nvidia/lib64

RUN wget https://gitlab.com/nvidia/container-images/cuda/-/raw/master/LICENSE?inline=false -O /NGC-DL-CONTAINER_LICENSE  #buildkit

ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility
ENV NV_CUDA_LIB_VERSION=11.4.2-1
ENV NV_NVTX_VERSION=11.4.120-1
ENV NV_LIBNPP_VERSION=11.4.0.110-1
ENV NV_LIBNPP_PACKAGE=libnpp-11-4=11.4.0.110-1
ENV NV_LIBCUSPARSE_VERSION=11.6.0.120-1
ENV NV_LIBCUBLAS_PACKAGE_NAME=libcublas-11-4
ENV NV_LIBCUBLAS_VERSION=11.6.1.51-1
ENV NV_LIBCUBLAS_PACKAGE=libcublas-11-4=11.6.1.51-1
ENV NV_LIBNCCL_PACKAGE_NAME=libnccl2
ENV NV_LIBNCCL_PACKAGE_VERSION=2.11.4-1
ENV NCCL_VERSION=2.11.4-1
ENV NV_LIBNCCL_PACKAGE=libnccl2=2.11.4-1+cuda11.4
ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends \
    cuda-libraries-11-4=${NV_CUDA_LIB_VERSION}     ${NV_LIBNPP_PACKAGE}     cuda-nvtx-11-4=${NV_NVTX_VERSION} \
    libcusparse-11-4=${NV_LIBCUSPARSE_VERSION}     ${NV_LIBCUBLAS_PACKAGE}     ${NV_LIBNCCL_PACKAGE} \
    && rm -rf /var/lib/apt/lists/* # buildkit

RUN apt-mark hold ${NV_LIBCUBLAS_PACKAGE_NAME} ${NV_LIBNCCL_PACKAGE_NAME} # buildkit


# TensorFlow with GPU support
# Reference https://github.com/tensorflow/tensorflow/blob/master/tensorflow/tools/dockerfiles/dockerfiles/gpu.Dockerfile
# But above is for CUDA 11.2. We replace 11.2 with 11.4
# For CUDNN and LIBNVINFER versions reference https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/

RUN apt clean && \
    apt autoclean && \
    apt install -f && \
    apt autoremove && \
    apt-get update

RUN apt-get install -y python3-pip --fix-missing
RUN python3 -m pip --no-cache-dir install --upgrade "pip<20.3" \
    setuptools
# Some TF tools expect a "python" binary
RUN ln -s $(which python3) /usr/local/bin/python

RUN pip3 install       \
    scikit-learn       \
    statsmodels        \
    networkx

# Reference: https://github.com/tensorflow/tensorflow/blob/master/tensorflow/tools/dockerfiles/dockerfiles/gpu.Dockerfile

ARG ARCH
ARG CUDA=11.4
ARG CUDNN=8.2.4.15-1
ARG CUDNN_MAJOR_VERSION=8
ARG LIB_DIR_PREFIX=x86_64
ARG LIBNVINFER=8.2.3-1
ARG LIBNVINFER_MAJOR_VERSION=8

# Needed for string substitution
SHELL ["/bin/bash", "-c"]
# Pick up some TF dependencies
RUN apt-get update -y && apt-get install -y --no-install-recommends --allow-change-held-packages \
        build-essential \
        cuda-command-line-tools-${CUDA/./-} \
        libcublas-${CUDA/./-} \
        cuda-nvrtc-${CUDA/./-} \
        libcufft-${CUDA/./-} \
        libcurand-${CUDA/./-} \
        libcusolver-${CUDA/./-} \
        libcusparse-${CUDA/./-} \
        curl \
        libcudnn8=${CUDNN}+cuda${CUDA} \
        libfreetype6-dev \
        libhdf5-serial-dev \
        libzmq3-dev \
        pkg-config \
        software-properties-common \
        unzip


# Install TensorRT if not building for PowerPC
# NOTE: libnvinfer uses cuda11.1 versions
RUN [[ "${ARCH}" = "ppc64le" ]] || { apt-get update && \
        apt-get install -y --no-install-recommends libnvinfer${LIBNVINFER_MAJOR_VERSION}=${LIBNVINFER}+cuda11.4 \
        libnvinfer-plugin${LIBNVINFER_MAJOR_VERSION}=${LIBNVINFER}+cuda11.4 \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*; }

# For CUDA profiling, TensorFlow requires CUPTI.
ENV LD_LIBRARY_PATH /usr/local/cuda/extras/CUPTI/lib64:/usr/local/cuda/lib64:$LD_LIBRARY_PATH

# Link the libcuda stub to the location where tensorflow is searching for it and reconfigure
# dynamic linker run-time bindings
RUN ln -s /usr/local/cuda/lib64/stubs/libcuda.so /usr/local/cuda/lib64/stubs/libcuda.so.1 \
    && echo "/usr/local/cuda/lib64/stubs" > /etc/ld.so.conf.d/z-cuda-stubs.conf \
    && ldconfig

# See http://bugs.python.org/issue19846
ENV LANG C.UTF-8


# Options:
#   tensorflow
#   tensorflow-gpu
#   tf-nightly
#   tf-nightly-gpu
# Set --build-arg TF_PACKAGE_VERSION=1.11.0rc0 to install a specific version.
# Installs the latest version by default.
ARG TF_PACKAGE=tensorflow
#ARG TF_PACKAGE_VERSION=2.6.0
#RUN python3 -m pip install --no-cache-dir ${TF_PACKAGE}${TF_PACKAGE_VERSION:+==${TF_PACKAGE_VERSION}}
RUN python3 -m pip install --no-cache-dir ${TF_PACKAGE}

USER hpcc
