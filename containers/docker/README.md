# HPCC Platform Community Version
  ubuntu (bionic)/
  -  base/<version>/Dockerfile: HPCC Platform Community version prequisites
     Docker Hub:  hpccsystems/hpcc-base
  -  platform/<version>/Dockerfile  HPCC Platform Community Docker build file
     Docker Hub:  hpccsystems/platform
  -  clienttools/<version>Dockerfile: HPCC Clienttools Community Docker build file
     Docker Hub: hpccsystems/clienttools

## How to buid
Create HPCC Platform Docker image :
```console
sudo docker build -t hpccsystems/platform:7.4.8-1 --build-arg version=7.4.8-1 .
```


# HPCC Platform Internal Version
  Reference LN container/docker/

# Development and custom Build
  <bionic|disco|eoan>/
  -   base/Dockerfile: HPCC Platform community version prequisites
  -   dev/<version>/Dockerfile: Build Server Docker build file
  -   platform/<version>/Dockerfile: Docker build file for compiling and building HPCC Platform image
  -   clienttools/<version>/Dockerfile: Docker build file for compiling and building HPCC Clienttools image

  gcc/<7|8|9>/
  -   base/Dockerfile: HPCC Platform community version prequisites
  -   dev/<version>/Dockerfile: Build Server Docker build file
  -   platform/<version>/Dockerfile: Docker build file for compiling and building HPCC Platform image

## How to build
  The compiling and building Docker build file has "ARG" for various input parameter.

Build master branch:
```console
sudo docker build -t hpccsystems/platform:master .
```

Build branch or tag, for example community_7.4.10-rc1:
```console
sudo docker build -t hpccsystems/platform:7.4.10-rc1 --build-arg branch=community_7.4.10-rc1 .
```

Build your own repo and branch:
```console
sudo docker build -t <your docker hub name>/platform:<your branch> --build-arg owner=<your github account> --build-arg branch=<your branch> .
```

Build a private repo:
```console
sudo docker build -t  <your docker hub name>/platform:<branch> --build-arg owner=<owner of repo> --build-arg branch=<branch> --build-arg user=<username> --build-arg password=<password>  .
```
