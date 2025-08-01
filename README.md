# Description / Rationale

HPCC Systems offers an enterprise ready, open source supercomputing platform to solve big data problems. As compared to Hadoop, the platform offers analysis of big data using less code and less nodes for greater efficiencies and offers a single programming language, a single platform and a single architecture for efficient processing. HPCC Systems is a technology division of LexisNexis Risk Solutions.

# Getting Started

* [Learn about HPCC](https://hpccsystems.com/about#Platform)
* [Download](https://hpccsystems.com/download)
* [Installation and Running](https://hpccsystems.com/training/documentation/installation-and-administration)
* [Developer Documentation](https://hpcc-systems.github.io/HPCC-Platform/)

# Release + Support Policy

In general, a new version of the HPCC Platform is released every 3 months. These releases can be either Major (with breaking changes) or Minor (with new features). Maintenance and security releases (point releases) are typically made weekly, and may occasionally include technical previews.

Maintenance releases are supported for the current and previous release, while security releases are supported for the current and previous two releases:

```mermaid
---
displayMode: compact
---
gantt
    title Release Schedule
    axisFormat %Y-Q%q
    tickInterval 3month
    dateFormat YYYY-MM-DD
    section v9.2.x
        Active:          active, 2023-07-04, 9M
        Critical:        3M
        Security:        3M
    section v9.4.x
        Active:          active, 2023-10-04, 9M
        Critical:        3M
        Security:        6M
    section v9.6.x
        Active:          active, 2024-04-04, 6M
        Critical:        3M
        Security:        6M
    section v9.8.x
        Active:          active, 2024-07-02, 9M
        Critical:        3M
        Security:        3M
    section v9.10.x
        Active:          active, 2025-01-01, 6M
        Critical:        3M
        Security:        3M
    section v9.12.x
        Active:          active, 2025-05-01, 5M
        Critical:        3M
        Security:        3M
    section v9.14.x
        Active:          active, 2025-07-01, 6M
        Critical:        3M
        Security:        3M
    section v10.0.x
        Expected:        milestone, 2025-10-01, 0M
```

# Architecture

The HPCC Systems architecture incorporates the Thor and Roxie clusters as well as common middleware components, an external communications layer, client interfaces which provide both end-user services and system management tools, and auxiliary components to support monitoring and to facilitate loading and storing of filesystem data from external sources. An HPCC environment can include only Thor clusters, or both Thor and Roxie clusters. Each of these cluster types is described in more detail in the following sections below the architecture diagram.

## Thor

Thor (the Data Refinery Cluster) is responsible for consuming vast amounts of data, transforming, linking and indexing that data. It functions as a distributed file system with parallel processing power spread across the nodes. A cluster can scale from a single node to thousands of nodes.

* Single job at a time
* Distributed parallel processing
* Distributed file system
* Powerful parallel processing programming language (ECL)
* Optimized for Extraction, Transformation, Loading, Sorting, Indexing and Linking
* Scales from 1-1000s of nodes

## Roxie

Roxie (the Query Cluster) provides separate high-performance online query processing and data warehouse capabilities.  Roxie (Rapid Online XML Inquiry Engine) is the data delivery engine used in HPCC to serve data quickly and can support many thousands of requests per node per second. 

* Multiple queries in parallel
* Distributed parallel processing
* Distributed file system
* Powerful parallel processing programming language (ECL)
* Optimized for concurrent query processing
* Scales from 1-1000s of nodes

## ECL

ECL (Enterprise Control Language) is the powerful programming language that is ideally suited for the manipulation of Big Data.

* Transparent and implicitly parallel programming language
* Non-procedural and dataflow oriented
* Modular, reusable, extensible syntax
* Combines data representation and algorithm implementation
* Easily extend using C++ libraries
* ECL is compiled into optimized C++

## ECL IDE

ECL IDE is a modern IDE used to code, debug and monitor ECL programs.

* Access to shared source code repositories
* Complete development, debugging and testing environment for developing ECL dataflow programs
* Access to the ECLWatch tool is built-in, allowing developers to watch job graphs as they are executing
* Access to current and historical job workunits

## ESP

ESP (Enterprise Services Platform) provides an easy to use interface to access ECL queries using XML, HTTP, SOAP and REST.

* Standards-based interface to access ECL functions

# Developer documentation

The following links describe the structure of the system and detail some of the key components:

* [An overview of workunits and the different stages in executing a query](https://hpcc-systems.github.io/HPCC-Platform/devdoc/Workunits.html)
* [An introduction to the code generator - eclcc](https://hpcc-systems.github.io/HPCC-Platform/devdoc/CodeGenerator.html)
* [The memory manager used by roxie and thor](https://hpcc-systems.github.io/HPCC-Platform/devdoc/MemoryManager.html)
* [The structure of the initialization scripts](https://hpcc-systems.github.io/HPCC-Platform/initfiles/DOCUMENTATION.html)
* [Outline of ecl-bundle](https://hpcc-systems.github.io/HPCC-Platform/ecl/ecl-bundle/DOCUMENTATION.html)
* [The structure and some details of the cmake files](https://hpcc-systems.github.io/HPCC-Platform/cmake_modules/DOCUMENTATION.html)
* [Building the documentation](https://hpcc-systems.github.io/HPCC-Platform/docs/DOCUMENTATION.html)

## Regression test

```sh
cd /opt/HPCCSystems/testing/regress
./ecl-test query --target thor nlppp.ecl
```