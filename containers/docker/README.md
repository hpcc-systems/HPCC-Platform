## HPCC Systems Platform Community Version
  platform/
  -  base/Dockerfile: HPCC Systems Platform Community version prequisites (Ubuntu 18.04)
     Docker Hub:  hpccsystems/hpcc-base
  -  Dockerfile  HPCC Systems Platform Community version Docker build file with version as input argument.
     Dockerfile-local  HPCC Systems Platform Community version Docker build file with local Platform package.
     Docker Hub:  hpccsystems/platform

  clienttools/
  -  Dockerfile: HPCC Systems Clienttools Community Docker version build file with version as input argument
     Docker Hub: hpccsystems/clienttools

  plugins/
  -  Dockerfile: HPCC Systems Plugins Community version Docker build file (based Platform Docker image) with version as input argument


### How to build
Create HPCC Systems Platform Docker image :
For example:
```console
sudo docker build -t hpccsystems/platform:7.6.6-1 --build-arg version=7.6.6-1 .
```
### How to test
```console
cd test
./test-build.sh -t 7.6.6-1
```


## Development and custom Build
  dev/
    &lt;ubuntu|gcc&gt;/
  -   base/Dockerfile: HPCC Systems Platform community version prequisites
  -   bldsvr/Dockerfile: Build Server Docker build file
  -   platform/Dockerfile: Docker build file for compiling and building HPCC Systems Platform image

### How to build
  The compiling and building Docker build file has "ARG" for various input parameter.

Build master branch:
```console
sudo docker build -t hpccsystems/platform:master .
```

Build branch or tag, for example community_7.6.6-1:
```console
sudo docker build -t hpccsystems/platform:7.6.6-1 --build-arg branch=community_7.6.6-1 .
```

Build your own repo and branch:
```console
sudo docker build -t <your docker hub name>/platform:<your branch> --build-arg owner=<your github account> --build-arg branch=<your branch> .
```

Build a private repo:
```console
sudo docker build -t  <your docker hub name>/platform:<branch> --build-arg owner=<owner of repo> --build-arg branch=<branch>--build-arg user=<username> --build-arg password=<password> .
```
