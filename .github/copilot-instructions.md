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

### Code Style (Essential Rules)
- Use Allman brace style, but allow single-line blocks to have no braces, unless nested.
- Use camelCase for variable names
- Use constexpr over macros
- No trailing whitespace
- Use `#pragma once` for header guards
- Use `Owned<>` vs `Linked<>` for object ownership
- Avoid default parameters (use method overloading instead)
- Use `%u` for unsigned integers, `%d` for signed integers
- Complete style guide: `devdoc/StyleGuide.md`

### Development Workflow
- See `devdoc/Development.md` for testing and development guidelines
- Use pull requests with code reviews
- Target appropriate branch per `devdoc/VersionSupport.md`

## Code Review Priorities

### HPCC Platform Specific Considerations
- **ECL Language Changes**: Verify syntax compatibility and backward compatibility with existing ECL code
- **Distributed System Impact**: Consider interactions between Dali, Thor, and Roxie components
- **Memory Management**: Check `Owned<>`/`Linked<>` usage and resource cleanup patterns
- **Thread Safety**: Look for race conditions, proper locking, and synchronization issues across all components
- **Performance Impact**: Consider effects on query execution and distributed data throughput
- **Security**: Validate input sanitization, authentication, and authorization mechanisms
- **API Compatibility**: Ensure changes don't break existing client interfaces

### Critical Quality Checks
1. **Memory Management**: Verify proper `Owned<>`/`Linked<>` usage and check for memory/resource leaks in general
2. **Thread Safety**: Especially in server components - look for race conditions and proper locking
3. **Error Handling**: Consistent error reporting and logging throughout the codebase
4. **Test Coverage**: Appropriate unit tests and integration tests for the change scope
5. **Documentation**: API documentation for public interfaces and significant behavior changes
6. **Style Compliance**: Adherence to coding standards in `devdoc/StyleGuide.md`

### Review Questions to Consider
- Are there any efficiency concerns or performance bottlenecks?
- Is the code thread-safe and properly synchronized?
- Could the code be refactored to improve maintainability and reuse?
- Are there any memory or resource leaks?
- Does this change maintain backward compatibility?
- Are error conditions properly handled and logged?
- Is the change properly tested with appropriate test coverage?

## TypeScript/React Project

For the ECL Watch web interface, see the separate instructions in `esp/src/.github/instructions/general.instructions.md`.

## Documentation Resources
- Code review guidelines: `devdoc/CodeReviews.md`
- Code submission process: `devdoc/CodeSubmissions.md`
- Contributing docs: `devdoc/docs/ContributeDocs.md`
- GitHub Copilot tips: `devdoc/userdoc/copilot/CopilotPromptTips.md`

## Location of other instruction files

For understanding the global metrics, see `.github/instructions/globalmetrics.md`.
