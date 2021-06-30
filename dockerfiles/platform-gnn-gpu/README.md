# HPCC Systems Machine Learning GNN GPU Docker Images

## Tensorflow 2 with GPU support

The new Tensorflow 2 and Nvidia Cuda libraries are not always compatible. It will be ideal if we can use Docker image by published by Tensorflow (Docker Hub tensorflow/tensorflow <version>-gpu) but the base image is Ubuntu 18.04 and we use Ubuntu 20.04. So instead we create the images by addiing Tensorflow 2 and Nvida Cuda, etc libraries on the top of hpccystems/platform-core image.

When preparing the Dockerfile, specially for Tensorflow 2 with GPU support it is important to reference
Tensorflow and Nvidia Cuda Dockerfiles and Docker images to pick compatible libabries.

### Docker image and Dockerfile Reference

#### Tensforflow

- Dockerfile:
  https://github.com/tensorflow/tensorflow/blob/master/tensorflow/tools/dockerfiles/dockerfiles/gpu.Dockerfile
  Above Dockeerfile is based https://hub.docker.com/layers/nvidia/cuda/. There are runime, base, etc.
  For now we just reference https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/ and replace the package version for Ubuntu 20.04

- The generated tensorflow/tensorflow:2.2.0-gpu
  https://hub.docker.com/layers/tensorflow/tensorflow/2.2.0-gpu/images/sha256-3f8f06cdfbc09c54568f191bbc54419b348ecc08dc5e031a53c22c6bba0a252e?context=explore

#### Nvidia Cuda

- NVDA CUDA Dockerfile: https://gitlab.com/nvidia/container-images/cuda/-/blob/master/dist/ubuntu20.04/

  https://hub.docker.com/layers/nvidia/cuda/11.3.1-base/images/sha256-351d731f46159c1afdb003121e96b512a77c410c2b43ee6ea74cf1413bb858ab?context=explore

- NGC-DL-CONTAINER-LICENSE
  https://gitlab.com/nvidia/container-images/cuda/-/blob/master/LICENSE under BSD 3-Clause "New" or "Revised" License


## Build


Go to ml images under <HPCC Platform>/dockerfiles/ml directory:

```console
export INPUT_BUILD_ML= <one of ml, gnn and gnn-gpu>
./buildall.sh
```

You can provide "-l" to build without version information as a default image.
You need manually push the image to Docker repository such as Docker Hub.

Machine Learning features can also be built with buildall.sh when environment variable BUILD_ML is defined. To build all set BUILD_ML=all. To build individual or subset set, for exampl, BUILD_ML=gnn or BUILD_ML="gnn gnn-gpu".
