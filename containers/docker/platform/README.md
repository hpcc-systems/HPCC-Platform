# Dockerfile links
-   [latest](https://github.com/hpcc-systems/containers/docker/tree/platform/Dockerfile)

# Quick reference
-   **Where to get help**: [HPCCSystems Forum](https://hpccsystems.com/bb/)
-   **Where to file issues**: [JIRA HPCC](https://track.hpccsystems.com/projects/HPCC/summary)
-   **Maintained by**:
builds@hpccsystems.com
-   **Source of this project**: [https://github.com/hpcc-systems/HPCC-Platform](https://github.com/hpcc-systems/HPCC-Platform)


# What is HPCC Systems(r)

The HPCC Systems server platform is a free, open source, massively scalable platform for big data analytics. Download the HPCC Systems server platform now and take the reins of the same core technology that LexisNexis has used for over a decade to analyze massive data sets for its customers in industry, law enforcement, government, and science.

For more information and related downloads for HPCC Systems products, please visit
https://hpccsystems.com

![alt text](https://hpccsystems.com/sites/default/files/hpcc-systems-horiz.png "HPCC Systems Logo")

# How to use this image
You can start the Docker HPCC Systems image in interactive (-i -t) or daemon mode (-d). You must start the HPCC Systems processes then go to ECLWatch to submit jobs, query, and explore your data with the HPCC Systems platform.

To start Docker on in interactive mode :
```console
sudo docker run -t -i  hpccsystems/platform  /bin/bash
```
For start CentOS Docker image
```console
sudo docker run -t -i --cap-add SYS_RESOURCE -e "container=docker"  <docker image>  /bin/bash
```

To start Docker in daemon mode
```console
sudo docker run -d hpccsystems/platform
```


# How is this image build
```console
sudo docker build -t <target image>:<tag> --build-arg version=<HPCC Systems Version> .
```
For example,
```console
sudo docker build -t hpccsystems/platform:7.6.6-1 --build-arg version=7.6.6-1  .
```

If you have local HPCC Systems Platform package you can use Dockerfile-local. It assumes that there is package with prefix hpccsystems-platform on local directory.

```console
sudo docker build -t <target image>:<tag> -f Dockerfile-local .
```
# License
Licensed under the Apache License, Version 2.0 (the "License")
