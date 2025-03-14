# Docker Images

_This document describes how to create and use a docker image for the HPCC-Platform._

## Creating a Docker Image

The HPCC-Platform repository contains a Dockerfile that can be used to create a docker image for the HPCC-Platform including all the required dependencies for a community edition.  The Dockerfile is located in [dockerfiles/vcpkg/platform-core-ubuntu-22.04](../dockerfiles/vcpkg/platform-core-ubuntu-22.04/Dockerfile) and is named `Dockerfile`.  The image can then be used to run the HPCC-Platform in a docker container.

### tldr;

_Too long; didn't read_

```sh
mkdir tmp
cd tmp
wget https://raw.githubusercontent.com/hpcc-systems/HPCC-Platform/master/dockerfiles/vcpkg/platform-core-ubuntu-22.04/Dockerfile
wget https://github.com/hpcc-systems/HPCC-Platform/releases/download/community_9.8.10-1/hpccsystems-platform-community_9.8.10-1jammy_amd64_withsymbols.deb
docker build --build-arg PKG_FILE=./hpccsystems-platform-community_9.8.10-1jammy_amd64_withsymbols.deb -t my-platform-image .
```

### Prerequisites

1. To create the docker image, you need to have Docker installed on your machine.  You can download Docker from the [Docker website](https://www.docker.com/).
2. You will need to have appropiate HPCC-Platform installation package, in this example the target is Ubuntu 22.04, so would need the HPCC-Platform Ubuntu 22.04 deb package.

### Building the Docker Image

1. Create a new empty folder
2. Copy the [Dockerfile](../dockerfiles/vcpkg/platform-core-ubuntu-22.04/Dockerfile) into this new folder.
3. Download the appropriate HPCC-Platform installation package and copy it into this new folder, for example:
  * [hpccsystems-platform-community_9.8.10-1jammy_amd64_k8s.deb](https://github.com/hpcc-systems/HPCC-Platform/releases/download/community_9.8.10-1/hpccsystems-platform-community_9.8.10-1jammy_amd64_k8s.deb)
  * [hpccsystems-platform-community_9.8.10-1jammy_amd64_withsymbols.deb](https://github.com/hpcc-systems/HPCC-Platform/releases/download/community_9.8.10-1/hpccsystems-platform-community_9.8.10-1jammy_amd64_withsymbols.deb)
4. Open a terminal window and navigate to the folder containing the Dockerfile and the HPCC-Platform installation package.
5. Run the following command to build the docker image passing the path to the HPCC-Platform installation package as a build argument:
```sh
docker build --build-arg PKG_FILE=<path to HPCC-Platform installation package> -t my-platform-image .
```

### Running the Docker Image

Once the docker image has been created, you can run it in a variety of ways, for example:

```sh
docker run -p 80:8010 my-platform-image sh -c "/etc/init.d/hpcc-init start && tail -f /var/log/HPCCSystems/myesp/esp.log"
```
