---
applyTo: 'esp/**'
---

# Instructions for ESP Services

## Overview

- ESP services consist of an interface description (ESDL) (`*.ecm`) file and a service implementation in c++.
- The service interface description (ESDL) is compiled into c++ code placed in the `generated` subdirectory of the build output directory specified by the VSCode setting `cmake.buildDirectory` for this project.
- In addition to these instructions, use the [esp service interface instructions](esp-service-interface.instructions.md) for changes that affect the API or service interface.

## Key Directories

- `esp/services` : Implementations of ESP services.
- `esp/scm` : Interface (API) descriptions (ESDL) for ESP services.

## General Instructions

- Version changes must be backward compatible with all previous versions.
- Check the version on the request by calling the `IEspContext::getClientVersion()` method.
