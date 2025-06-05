---
applyTo: "**"
---
# GitHub Copilot Instructions for HPCC Platform

## Repository Structure

This repository contains **two distinct projects**:

1. **C++ Project** (this root folder): The main HPCC Platform system
2. **TypeScript/React Project** (`esp/src/`): ECL Watch web interface

## C++ Project (Root Folder)

### Build System
- **Build tool**: CMake with Ninja generator
- **Configuration**: Use options from `vcpkg-linux.code-workspace`
- **Key cmake arguments**:
  ```
  -DCONTAINERIZED=OFF
  -DUSE_OPTIONAL=OFF
  -DUSE_CPPUNIT=ON
  -DINCLUDE_PLUGINS=ON
  -DSUPPRESS_V8EMBED=ON
  -DSUPPRESS_REMBED=ON
  ```

### Build Commands
```bash
# Configure the build
cmake -B ./build -S . -G Ninja -DCONTAINERIZED=OFF -DUSE_OPTIONAL=OFF -DUSE_CPPUNIT=ON -DINCLUDE_PLUGINS=ON -DSUPPRESS_V8EMBED=ON -DSUPPRESS_REMBED=ON -DCMAKE_BUILD_TYPE=Debug

# Build
cmake --build ./build --parallel

# Create package
cmake --build ./build --parallel --target package
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

### Code Style
- Follow the style guide in `devdoc/StyleGuide.md`
- Review guidelines in `devdoc/CodeReviews.md`
- Submission process in `devdoc/CodeSubmissions.md`

### Development Workflow
- See `devdoc/Development.md` for testing and development guidelines
- Use pull requests with code reviews
- Target appropriate branch per `devdoc/VersionSupport.md`

## TypeScript/React Project

For the ECL Watch web interface, see the separate instructions in `esp/src/.github/instructions/general.instructions.md`.

## Documentation
- Contributing docs: `devdoc/docs/ContributeDocs.md`
- GitHub Copilot tips: `devdoc/userdoc/copilot/CopilotPromptTips.md`
