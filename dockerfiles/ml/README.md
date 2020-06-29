# HPCC Systems Machine Learning Docker Images

## Current Machine Learning Features

- ml:  Scikit-Learning
- gnn:     Tensorflow 2 + Scikit-Learning
- gnn-gpu: Tensorflow 2 with GPU support + Scikit-Learning

## GNN and Tensorflow

Current HPCC Systems GNN officially support Tensorflow version 1.x. But latest HPCC Systems Docker images are based on Ubuntu 20.04 and linked Python 3.8 libraries which doesn't support Tensorflow version 1.x.  To use Tensorflow 2 with GNN creating Tensorflow session as:

```code
import tensorflow as tf
s = tf.compat.v1.Session()
```

## Tensorflow 2 with GPU support

The new Tensorflow 2 and Nvidia Cuda libraries are not always compatible. It will be ideal if we can use Docker image by published by Tensorflow but the base image is Ubuntu 18.04 and we use Ubuntu 20.04. So instead we create the images by addiing Tensorflow 2 and Nvida Cuda, etc libraries on the top of hpccystems/platform-core image.

When preparing the Dockerfile, specially for Tensorflow 2 with GPU support it is import to reference
Tensorflow and Nvidia Cuda Dockerfiles and Docker images to pick compatible libabries.

### Docker image and Dockerfile Reference

#### Tensforflow

- Dockerfile:
  https://github.com/tensorflow/tensorflow/blob/master/tensorflow/tools/dockerfiles/dockerfiles/gpu.Dockerfile
- tensorflow/tensorflow:2.2.0-gpu
   https://hub.docker.com/layers/tensorflow/tensorflow/2.2.0-gpu/images/sha256-3f8f06cdfbc09c54568f191bbc54419b348ecc08dc5e031a53c22c6bba0a252e?context=explore

#### Nvidia Cuda

- NVDA CUDA Dockerfile: https://gitlab.com/nvidia/container-images/cuda/-/blob/master/dist/ubuntu18.04/10.1/base/Dockerfile
- nvidia/cuda:10.1-base-ubuntu18.04:
  https://hub.docker.com/layers/nvidia/cuda/10.1-base-ubuntu18.04/images/sha256-3cb86d1437161ef6998c4a681f2ca4150368946cc8e09c5e5178e3598110539f?context=explore

## Build

Make sure desired hpccsystems/platform-core Docker image is avaiable otherwise this need be build first with buildall.sh script.

Go to ml images under <HPCC Platform>/dockerfiles/ml directory:

```console
./build -t <platform-core tag> -m <one of ml, gnn and gnn-gpu>
```

You can provide "-l" to build without version information as a default image.
You need manually push the image to Docker repository such as Docker Hub.

Machine Learning features can also be built with buildall.sh when environment variable BUILD_ML is defined. To build all set BUILD_ML=all. To build individual or subset set, for exampl, BUILD_ML=gnn or BUILD_ML="gnn gnn-gpu".
