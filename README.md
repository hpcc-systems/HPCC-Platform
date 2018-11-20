# Description / Rationale

HPCC Systems offers an enterprise ready, open source supercomputing platform to solve big data problems. As compared to Hadoop, the platform offers analysis of big data using less code and less nodes for greater efficiencies and offers a single programming language, a single platform and a single architecture for efficient processing. HPCC Systems is a technology division of LexisNexis Risk Solutions.

# Getting Started

* [Learn about HPCC](https://hpccsystems.com/about#Platform)
* [Download](https://hpccsystems.com/download)
* [Installation and Running](https://hpccsystems.com/training/documentation/installation-and-administration)
* [Build from Source](https://github.com/hpcc-systems/HPCC-Platform/wiki/Building-HPCC)

# Architecture

The HPCC Systems architecture incorporates the Thor and Roxie clusters as well as common middleware components, an external communications layer, client interfaces which provide both end-user services and system management tools, and auxiliary components to support monitoring and to facilitate loading and storing of filesystem data from external sources. An HPCC environment can include only Thor clusters, or both Thor and Roxie clusters. Each of these cluster types is described in more detail in the following sections below the architecture diagram.

## Thor

Thor (the Data Refinery Cluster) is responsible for consuming vast amounts of data, transforming, linking and indexing that data. It functions as a distributed file system with parallel processing power spread across the nodes. A cluster can scale from a single node to thousands of nodes.

* Single-threaded
* Distributed parallel processing
* Distributed file system
* Powerful parallel processing programming language (ECL)
* Optimized for Extraction, Transformation, Loading, Sorting, Indexing and Linking
* Scales from 1-1000s of nodes

## Roxie

Roxie (the Query Cluster) provides separate high-performance online query processing and data warehouse capabilities.  Roxie (Rapid Online XML Inquiry Engine) is the data delivery engine used in HPCC to serve data quickly and can support many thousands of requests per node per second. 

* Multi-threaded
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

* [An overview of workunits and the different stages in executing a query](https://github.com/hpcc-systems/HPCC-Platform/blob/master/ecl/eclcc/WORKUNITS.rst)
* [An introduction to the code generator - eclcc](https://github.com/hpcc-systems/HPCC-Platform/blob/master/ecl/eclcc/DOCUMENTATION.rst)
* [The memory manager used by roxie and thor](https://github.com/hpcc-systems/HPCC-Platform/blob/master/roxie/roxiemem/DOCUMENTATION.rst)
* [The structure of the initialization scripts](https://github.com/hpcc-systems/HPCC-Platform/blob/master/initfiles/DOCUMENTATION.rst)
* [Outline of ecl-bundle](https://github.com/hpcc-systems/HPCC-Platform/blob/master/ecl/ecl-bundle/DOCUMENTATION.rst)
* [The structure and some details of the cmake files](https://github.com/hpcc-systems/HPCC-Platform/blob/master/cmake_modules/DOCUMENTATION.rst)
* [Building the documentation](https://github.com/hpcc-systems/HPCC-Platform/blob/master/docs/DOCUMENTATION.rst)

# Tagging a release

Scripts to tag, update the changelog and push a release can be found in [package.json](./package.json) and are dependent on [standard-version](https://github.com/conventional-changelog/standard-version).

To install:
```
npm install
```

and to publish:
```
npm run publish
```

---

This process is dependent on specially formatted commit messages adhering to the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0-beta.2/#summary) specification and should be structured as follows:

---

```
<type>[optional scope]: <description>

[optional body]

[optional footer]
```
---

<br />
The commit contains the following structural elements, to communicate intent to the
consumers of your library:

1. **fix:** a commit of the _type_ `fix` patches a bug in your codebase (this correlates with [`PATCH`](http://semver.org/#summary) in semantic versioning).
1. **feat:** a commit of the _type_ `feat` introduces a new feature to the codebase (this correlates with [`MINOR`](http://semver.org/#summary) in semantic versioning).
1. **BREAKING CHANGE:** a commit that has the text `BREAKING CHANGE:` at the beginning of its optional body or footer section introduces a breaking API change (correlating with [`MAJOR`](http://semver.org/#summary) in semantic versioning).
A BREAKING CHANGE can be part of commits of any _type_.
1. Others: commit _types_ other than `fix:` and `feat:` are allowed, for example [commitlint-config-conventional](https://github.com/marionebl/commitlint/tree/master/%40commitlint/config-conventional) (based on the [the Angular convention](https://github.com/angular/angular/blob/22b96b9/CONTRIBUTING.md#-commit-message-guidelines)) recommends `chore:`, `docs:`, `style:`, `refactor:`, `perf:`, `test:`, and others.
We also recommend `improvement` for commits that improve a current implementation without adding a new feature or fixing a bug.
Notice these types are not mandated by the conventional commits specification, and have no implicit effect in semantic versioning (unless they include a BREAKING CHANGE).
<br />
A scope may be provided to a commit's type, to provide additional contextual information and is contained within parenthesis, e.g., `feat(parser): add ability to parse arrays`.

## Examples

### Commit message with description and breaking change in body
```
feat: allow provided config object to extend other configs

BREAKING CHANGE: `extends` key in config file is now used for extending other config files
```

### Commit message with no body
```
docs: correct spelling of CHANGELOG
```

### Commit message with scope
```
feat(lang): added polish language
```

### Commit message for a fix using an (optional) issue number.
```
fix: minor typos in code

see the issue for details on the typos fixed

fixes issue #12
```
## Specification

The key words “MUST”, “MUST NOT”, “REQUIRED”, “SHALL”, “SHALL NOT”, “SHOULD”, “SHOULD NOT”, “RECOMMENDED”, “MAY”, and “OPTIONAL” in this document are to be interpreted as described in [RFC 2119](https://www.ietf.org/rfc/rfc2119.txt).

1. Commits MUST be prefixed with a type, which consists of a noun, `feat`, `fix`, etc., followed by a colon and a space.
1. The type `feat` MUST be used when a commit adds a new feature to your application or library.
1. The type `fix` MUST be used when a commit represents a bug fix for your application.
1. An optional scope MAY be provided after a type. A scope is a phrase describing a section of the codebase enclosed in parenthesis, e.g., `fix(parser):`
1. A description MUST immediately follow the type/scope prefix.
The description is a short description of the code changes, e.g., _fix: array parsing issue when multiple spaces were contained in string._
1. A longer commit body MAY be provided after the short description, providing additional contextual information about the code changes. The body MUST begin one blank line after the description.
1. A footer MAY be provided one blank line after the body.
  The footer SHOULD contain additional issue references about the code changes (such as the issues it fixes, e.g.,`Fixes #13`).
1. Breaking changes MUST be indicated at the very beginning of the footer or body section of a commit. A breaking change MUST consist of the uppercase text `BREAKING CHANGE`, followed by a colon and a space.
1. A description MUST be provided after the `BREAKING CHANGE: `, describing what has changed about the API, e.g., _BREAKING CHANGE: environment variables now take precedence over config files._
1. The footer MUST only contain `BREAKING CHANGE`, external links, issue references, and other meta-information.
1. Types other than `feat` and `fix` MAY be used in your commit messages.

## Why Use Conventional Commits

* Automatically generating CHANGELOGs.
* Automatically determining a semantic version bump (based on the types of commits landed).
* Communicating the nature of changes to teammates, the public, and other stakeholders.
* Triggering build and publish processes.
* Making it easier for people to contribute to your projects, by allowing them to explore
  a more structured commit history.

