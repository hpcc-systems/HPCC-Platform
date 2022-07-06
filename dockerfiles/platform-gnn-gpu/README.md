# HPCC Systems Machine Learning GNN GPU Docker Images

## Tensorflow with GPU support

### Support Tensorflow on a GPU with Docker
Docker is the easiest way to run TensorFlow on a GPU since the host machine only requires the NVIDIA® driver (the NVIDIA® CUDA® Toolkit is not required).
When you create an AKS cluster with GPU support a VM sized NVIDIA driver is installed on the nodes.
Run "nvidia-smi" will print the NVIDIA driver version. Our HPCC GNN GPU Docker image, platform-gnn-gpu, will have NVIDIA CUDA Toolkit.
Run "nvcc --version" to display the version. Beware the compatibility issue. For example CUDA Toolkit 11.3 and earlier will not be compatible with NVIDIA Driver 11.4+.  Reference https://docs.nvidia.com/deploy/cuda-compatibility/index.html for more information.
Current NCS V3 node has  NVIDIA Driver 11.4.

### GPU support in our Azure subscription
Our Azure subscription does include VM Size of GPU support. These VM Sizes normally are much more expensive than normal VM Sizes, probably at least 10 times. Each of these VM Sizes with GPU support have different hardware and may require different driver versions, such as NVidia CUDA driver. Also the Tensorflow 2 and Nvidia Cuda libraries are not always compatible.

Generally  some ND, NCv2 or NCv3 family VM Sizes should be supported in our Azure subscription
For example, the following are supported: standard_nc6s_v3,standard_nc8as_t4_v3,standard_nv12s_v3,standard_nv24s_v3,standard_nv48s_v3

We currently build our Docker Image with NVidia CUDA 11.4 and the latest Tensorflow packages. Users may need to create their own Docker Images with a different CUDA driver version and Tensorflow version.

When preparing the Dockerfile, especially for Tensorflow 2 with GPU support it is important to reference Tensorflow and Nvidia Cuda Dockerfiles and Docker images to pick compatible libraries.

### Docker image and Dockerfile Reference

- https://docs.nvidia.com/grid/cloud-service-support.html
- https://docs.microsoft.com/en-us/azure/virtual-machines/linux/n-series-driver-setup
- https://docs.microsoft.com/en-us/azure/virtual-machines/extensions/hpccompute-gpu-linux
- NVIDIA GPU Cloud (NGC) https://www.nvidia.com/en-us/gpu-cloud/


#### Dockerfile Consideration
Tensorflow only provides CUDA 11.2 Dockerfile for GPU support: https://github.com/tensorflow/tensorflow/blob/master/tensorflow/tools/dockerfiles/dockerfiles/gpu.Dockerfile

When preparing our Dockerfile (CUDA 11.4) we reference the above gpu.Dockerfile with following modifications
- Apply nvidia/cuda settings from https://hub.docker.com/layers/cuda/nvidia/cuda/11.4.3-cudnn8-runtime-ubuntu20.04/images/sha256-3c53124b4cdd6d6b8f4f7cef889b939ee776709a2249a0eb394d228af7623da6?context=explore
- Use https://github.com/tensorflow/tensorflow/blob/master/tensorflow/tools/dockerfiles/dockerfiles/gpu.Dockerfile as template but replace CUDA 11.2 with 11.4.
- For CUDNN and LIBNVINFER versions reference https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/
- NGC-DL-CONTAINER-LICENSE https://gitlab.com/nvidia/container-images/cuda/-/blob/master/LICENSE under BSD 3-Clause "New" or "Revised" License

## Build
Go to <HPCC Platform>/dockerfiles/platform-gnn-gpu directory:

```console
#docker build --build-arg DOCKER_REPO=<Docker Image REPO> --build-arg BUILD_LABEL=<Major>.<Minor>.<Point> <Dockerfile directory>
#For example,
docker build --build-arg DOCKER_REPO=hpccsystems --build-arg BUILD_LABEL=8.8.0  .
```
## Test
Users can deploy HPCC Cluster with the Docker Image of HPCC GNN/GPU using either HPCC Terraform or Helm Charts
Or create a simple Deployment yaml file
```code
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    app: gnn-gpu
  name: gnn-gpu
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gnn-gpu
  template:
    metadata:
      labels:
        app: gnn-gpu
    spec:
      containers:
      - name: gnn-gpu
        image: <HPCC GNN GPU Docker image>
        imagePullPolicy: IfNotPresent
        command: ["sleep"]
        args: ["infinity" ]
        resources:
          limits:
            nvidia.com/gpu: 1
```
To deploy above definition:
```
kubectl apply -f <yaml name>
```

Access the container:
```console
kubectl exec -i -t <pod name> -- /bin/bash
```
### Test CUDA driver
```console
nvcc --version
nvidia-smi
```
### Test from Tensorflow
```console
python3
import tensorflow as tf
print(tf.test.is_gpu_available())
print(tf.test.is_gpu_available(cuda_only=True))
print(tf.reduce_sum(tf.random.normal([1000, 1000])))
```
