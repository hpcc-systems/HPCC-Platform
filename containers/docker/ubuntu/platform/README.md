# Supported tags and respective Dockerfile links
-   [7.4.8-1, 7.4, 7](https://github.com/hpcc-systems/HPCC-Platform/tree/candidate-7.4.x/containers/docker/ubuntu/clienttools/7.4/Dockerfile)

# Quick reference
-   **Where to get help**:
   [HPCCSystems Forum](https://hpccsystems.com/bb/)
-   **Where to file issues**:
   [JIRA HPCC](https://track.hpccsystems.com/projects/HPCC/summary)
-   **Maintained by**:
builds@hpccsystems.com
-   **Source of this project**:
   [https://github.com/hpcc-systems/HPCC-Platform](https://github.com/hpcc-systems/HPCC-Platform)


# What is HPCCSystems

The HPCC Systems server platform is a free, open source, massively scalable platform for big data analytics. Download the HPCC Systems server platform now and take the reins of the same core technology that LexisNexis has used for over a decade to analyze massive data sets for its customers in industry, law enforcement, government, and science.

For more information and related downloads for HPCC Systems products, please visit
https://hpccsystems.com

![alt text](https://hpccsystems.com/sites/default/files/hpcc-systems-horiz.png "HPCCSystems Logo")

# How to use this image
You can start the Docker HPCC image in interactive (-i -t) or daemon mode (-d). You must start the HPCC processes then go to ECLWatch to submit jobs, query, and explore your data with the HPCC Systems platform.

To start Docker in interactive mode :
```
sudo docker run -t -i  hpccsystems/clienttools  /bin/bash
```

To start Docker in daemon mode
```
sudo docker run -d hpccsystems/clienttools
```

# How is this image build
```console
sudo docker build -t <target image>:<tag> --build-arg version=<HPCC Version> .
```
For example,
```console
sudo docker build -t hpccsystems/clienttools:7.4 --build-arg version=7.4.8-1  .
```

# License
Licensed under the Apache License, Version 2.0 (the "License")
