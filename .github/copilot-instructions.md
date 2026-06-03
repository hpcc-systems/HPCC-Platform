# GitHub Copilot Instructions for HPCC Platform

## Repository Overview

This repository contains **two distinct projects**:

1. **C++ Project** (this root folder): The main HPCC Platform system
2. **TypeScript/React Project** (`esp/src/`): ECL Watch web interface

## C++ Project (Root Folder)

### Build System
- **Build tool**: CMake with Ninja generator
- **Configuration**: Use options from `vcpkg-linux.code-workspace`
- **Key cmake arguments**: Use the cmake options defined in .vscode/settings.json, e.g. from cmake.configureSettings, when building with cmake

### Build Commands
- **Build directory** (<build-dir>): Extract the current setting from "cmake.buildDirectory" in .vscode/settings.json which is based on $buildType
```bash
# Configure the build
cmake -B <build-dir> -S . -G Ninja -DCONTAINERIZED=OFF -DUSE_OPTIONAL=OFF -DUSE_CPPUNIT=ON -DINCLUDE_PLUGINS=ON -DSUPPRESS_V8EMBED=ON -DSUPPRESS_REMBED=ON -DCMAKE_BUILD_TYPE=Debug

# Build
cmake --build <build-dir> --parallel

# Create package
cmake --build <build-dir> --parallel --target package
```

### Key Directories
- `common/` - Common utilities and libraries
- `dali/` - Dali distributed server components
- `ecl/` - ECL compiler and language components
- `esp/` - Enterprise Services Platform (web services)
- `roxie/` - Roxie rapid data delivery engine
- `thorlcr/` - Thor large capacity resource (batch processing)
- `system/` - System-level components
- `rtl/` - Runtime library
- `devdoc/` - Developer documentation
- `testing/` - Test suites

### HPCC Platform Architecture
- **Dali**: Distributed metadata server (storage and coordination)
- **Thor**: Batch processing engine for large-scale data processing
- **Roxie**: Real-time query engine for rapid data delivery
- **ESP**: Enterprise Services Platform for web services and APIs
- **ECL**: Enterprise Control Language compiler and runtime


### Running Unit Tests
**Shared object/DLL**: If the tests are in a shared object/dll, run them using the `unittests` program with the unit test name as an argument and the `-v` option to show progress. The `unittests` runner auto-loads from its default adjacent `lib`/`libs` directories; if the test library is installed elsewhere, also pass `-d <directory>`. For example, plugin-installed tests such as `DMetaphoneTest` may need `unittests -d <install>/plugins DMetaphoneTest -v`.
- **Executable**: If the tests are in an exe, run the program with the `-selftest` argument (e.g., `roxie -selftest`).

### Development Workflow
- See `devdoc/Development.md` for testing and development guidelines
- Use pull requests with code reviews
- Target appropriate branch per `devdoc/VersionSupport.md`

## TypeScript/React Project

For the ECL Watch web interface, see the separate instructions in `esp/src/.github/instructions/general.instructions.md`.

## Documentation Resources
- Code review guidelines: `devdoc/CodeReviews.md`
- Code submission process: `devdoc/CodeSubmissions.md`
- Contributing docs: `devdoc/docs/ContributeDocs.md`
- GitHub Copilot tips: `devdoc/userdoc/copilot/CopilotPromptTips.md`

## Location of other instruction files

For understanding the global metrics, see `.github/instructions/globalmetrics.md`.
