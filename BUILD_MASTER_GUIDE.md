# Complete Build Guide for HPCC Platform Master Branch

This comprehensive guide provides step-by-step instructions for building the HPCC Platform from the master branch. It consolidates information from various sources and provides modern development workflows.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Prerequisites](#prerequisites)
3. [Getting the Source Code](#getting-the-source-code)
4. [Build Configuration](#build-configuration)
5. [Building the Platform](#building-the-platform)
6. [Creating Packages](#creating-packages)
7. [Testing Your Build](#testing-your-build)
8. [Development Workflows](#development-workflows)
9. [Troubleshooting](#troubleshooting)

## Quick Start

For experienced developers who want to get started quickly:

```bash
# 1. Install prerequisites (Ubuntu/Debian example)
sudo apt-get install cmake ninja-build git bison flex build-essential binutils-dev libldap2-dev libcppunit-dev libicu-dev libxslt1-dev zlib1g-dev libboost-regex-dev libarchive-dev python3-dev libv8-dev default-jdk libapr1-dev libaprutil1-dev libiberty-dev libhiredis-dev libtbb-dev libxalan-c-dev libnuma-dev nodejs libevent-dev libatlas-base-dev libblas-dev default-libmysqlclient-dev libsqlite3-dev libmemcached-dev libcurl4-openssl-dev pkg-config libtool autotools-dev automake libssl-dev

# 2. Get the source code with submodules
git clone --recurse-submodules https://github.com/hpcc-systems/HPCC-Platform.git
cd HPCC-Platform

# 3. Configure the build
mkdir build
cd build
cmake -B . -S .. -G Ninja -DCONTAINERIZED=OFF -DUSE_OPTIONAL=OFF -DUSE_CPPUNIT=ON -DINCLUDE_PLUGINS=ON -DSUPPRESS_V8EMBED=ON -DSUPPRESS_REMBED=ON -DCMAKE_BUILD_TYPE=Debug

# 4. Build
cmake --build . --parallel

# 5. Create package
cmake --build . --target package
```

## Prerequisites

The HPCC Platform requires several third-party tools and libraries. The minimum CMake version required is 3.3.2, but 3.15+ is recommended.

### Ubuntu/Debian

#### Ubuntu 22.04/20.04/18.04

```bash
sudo apt-get update
sudo apt-get install cmake ninja-build git bison flex build-essential binutils-dev libldap2-dev libcppunit-dev libicu-dev libxslt1-dev zlib1g-dev libboost-regex-dev libarchive-dev python3-dev libv8-dev default-jdk libapr1-dev libaprutil1-dev libiberty-dev libhiredis-dev libtbb-dev libxalan-c-dev libnuma-dev nodejs libevent-dev libatlas-base-dev libblas-dev default-libmysqlclient-dev libsqlite3-dev r-base-dev r-cran-rcpp r-cran-rinside r-cran-inline libmemcached-dev libcurl4-openssl-dev pkg-config libtool autotools-dev automake libssl-dev
```

#### Additional dependencies for full plugin support:

```bash
sudo apt-get install python3-dev python3-pip
# For R support
sudo apt-get install r-base-dev r-cran-rcpp r-cran-rinside r-cran-inline
```

### CentOS/RHEL/Fedora

#### CentOS 8/RHEL 8/Fedora 35+

```bash
# Enable EPEL repository first
sudo dnf install -y epel-release

sudo dnf install cmake ninja-build gcc-c++ gcc make bison flex binutils-devel openldap-devel libicu-devel libxslt-devel libarchive-devel boost-devel openssl-devel apr-devel apr-util-devel hiredis-devel numactl-devel mariadb-devel libevent-devel tbb-devel atlas-devel python3-devel libmemcached-devel sqlite-devel java-1.8.0-openjdk-devel nodejs libcurl-devel
```

### macOS

Using Homebrew:

```bash
brew install cmake ninja git bison flex binutils openldap icu4c boost libarchive openssl@1.1
# Ensure bison is in your PATH ahead of system bison
export PATH="/usr/local/opt/bison/bin:$PATH"
```

### Windows

The Windows build uses vcpkg for dependency management. Install the following:

1. **Visual Studio 2019 or later** with C++ development tools
2. **Git for Windows**
3. **Chocolatey** (optional, for additional tools)

```cmd
# Install flex/bison using chocolatey
choco install winflexbison3
```

## Getting the Source Code

### Clone the Repository

Always use `--recurse-submodules` to get all required submodules:

```bash
git clone --recurse-submodules https://github.com/hpcc-systems/HPCC-Platform.git
cd HPCC-Platform
```

If you already cloned without submodules, initialize them:

```bash
git submodule update --init --recursive
```

### Verify Submodules

Check that submodules are properly initialized:

```bash
git submodule status
# Should show:
# -<commit_hash> esp/src/dgrid
# -<commit_hash> vcpkg
```

## Build Configuration

### Recommended Development Configuration

Create a build directory and configure with CMake:

```bash
mkdir build
cd build

# Debug build with plugins for development
cmake -B . -S .. \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCONTAINERIZED=OFF \
  -DUSE_OPTIONAL=OFF \
  -DUSE_CPPUNIT=ON \
  -DINCLUDE_PLUGINS=ON \
  -DSUPPRESS_V8EMBED=ON \
  -DSUPPRESS_REMBED=ON
```

### Release Configuration

For production builds:

```bash
cmake -B . -S .. \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCONTAINERIZED=OFF \
  -DUSE_OPTIONAL=ON \
  -DINCLUDE_PLUGINS=OFF
```

### Common CMake Options

| Option | Default | Description |
|--------|---------|-------------|
| `CMAKE_BUILD_TYPE` | Release | `Debug`, `Release`, `RelWithDebInfo`, `MinSizeRel` |
| `CONTAINERIZED` | OFF | Build for container deployment |
| `USE_OPTIONAL` | ON | Automatically disable features with missing deps |
| `USE_CPPUNIT` | OFF | Enable unit tests (requires cppunit) |
| `INCLUDE_PLUGINS` | OFF | Include all plugins in build |
| `CLIENTTOOLS_ONLY` | OFF | Build only client tools |
| `PLATFORM` | ON | Build platform components |
| `SIGN_MODULES` | OFF | Enable ECL standard library signing |

### Plugin Configuration

Enable specific plugins:

```bash
# Enable specific plugins
cmake -B . -S .. \
  -DCASSANDRAEMBED=ON \
  -DMYSQLEMBED=ON \
  -DKAFKA=ON \
  -DPYEMBED=ON
```

Available plugins:
- `CASSANDRAEMBED` - Cassandra database support
- `MONGODBEMBED` - MongoDB support  
- `MYSQLEMBED` - MySQL support
- `KAFKA` - Apache Kafka support
- `PYEMBED` - Python embedding
- `JAVAEMBED` - Java embedding
- `REDIS` - Redis support
- `MEMCACHED` - Memcached support
- `SQLITE3EMBED` - SQLite support
- `REMBED` - R language support
- `V8EMBED` - JavaScript V8 support

## Building the Platform

### Using CMake Build Command (Recommended)

```bash
# Build with all available cores
cmake --build . --parallel

# Build with specific number of cores
cmake --build . --parallel 6

# Build specific target
cmake --build . --target ecl
```

### Using Native Build Tools

If you prefer using ninja directly:

```bash
ninja          # Build everything
ninja ecl      # Build specific target
ninja -j6      # Build with 6 parallel jobs
```

### Build Targets

Common build targets:
- Default (no target) - Build everything
- `ecl` - ECL compiler
- `eclcc` - ECL command-line compiler
- `roxie` - Roxie server
- `thor` - Thor cluster
- `dali` - Dali server
- `package` - Create installation package

## Creating Packages

### Create Installation Package

```bash
# Create appropriate package for your system (DEB, RPM, or TGZ)
cmake --build . --target package

# Alternative using make/ninja
ninja package
```

The package will be created in your build directory with a name like:
- Ubuntu/Debian: `hpccsystems-platform-community_X.X.X-trunk0_amd64.deb`
- CentOS/RHEL: `hpccsystems-platform-community-X.X.X-trunk0.x86_64.rpm`
- Others: `hpccsystems-platform-community-X.X.X-trunk0-Linux.tar.gz`

### Install the Package

#### Ubuntu/Debian
```bash
sudo dpkg -i hpccsystems-platform-community_*.deb
# Fix any missing dependencies
sudo apt-get -f install
```

#### CentOS/RHEL/Fedora
```bash
sudo rpm -ivh hpccsystems-platform-community-*.rpm
# Or with dnf/yum
sudo dnf localinstall hpccsystems-platform-community-*.rpm
```

## Testing Your Build

### Unit Tests

If you built with `-DUSE_CPPUNIT=ON`:

```bash
cd build
ctest --output-on-failure --parallel 4
```

### Component Self-Tests

Test individual components:

```bash
# From build directory
./Debug/bin/roxie -selftest
./Debug/bin/eclagent -selftest
./Debug/bin/daregress localhost
```

### ECL Compiler Test

Test the ECL compiler:

```bash
echo "OUTPUT('Hello, World!');" > test.ecl
./Debug/bin/eclcc test.ecl
```

### Starting Services

After installing the package:

```bash
# Start all HPCC services
sudo systemctl start hpcc-platform

# Check status
sudo systemctl status hpcc-platform

# View ECL Watch (web interface)
# Open browser to http://localhost:8010
```

## Development Workflows

### Using VS Code

Open the repository in VS Code with the recommended workspace:

```bash
code vcpkg-linux.code-workspace
```

This workspace includes:
- CMake Tools integration
- Recommended build configuration
- C++ development extensions

### Debug Builds

For debugging with GDB or other debuggers:

```bash
cmake -B build-debug -S . \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DUSE_CPPUNIT=ON

cmake --build build-debug --parallel
```

### Client Tools Only

To build only the client tools (useful for development machines):

```bash
cmake -B build-client -S . \
  -G Ninja \
  -DCLIENTTOOLS_ONLY=ON

cmake --build build-client --parallel
```

### Containerized Development

For container-based deployment:

```bash
cmake -B build-container -S . \
  -G Ninja \
  -DCONTAINERIZED=ON

cmake --build build-container --parallel
```

### Documentation

To build documentation (requires additional tools):

```bash
# Install documentation prerequisites
sudo apt-get install docbook xsltproc fop

# Configure with docs
cmake -B . -S .. -DMAKE_DOCS=ON

# Build docs
cmake --build . --target docs
```

## Troubleshooting

### Common Issues

#### Missing Dependencies
If CMake configuration fails due to missing dependencies:

1. Install the missing packages listed in the error
2. Use `-DUSE_OPTIONAL=ON` to skip optional dependencies
3. Disable specific plugins causing issues with `-DSUPPRESS_<PLUGIN>=ON`

#### vcpkg Bootstrap Issues
If vcpkg bootstrap fails:

```bash
cd vcpkg
./bootstrap-vcpkg.sh
```

#### Build Failures
- **Out of memory**: Reduce parallel jobs with `--parallel 2`
- **Missing headers**: Ensure all prerequisites are installed
- **Permission errors**: Check file permissions and ownership

#### Plugin Issues
Disable problematic plugins:

```bash
cmake -B . -S .. -DSUPPRESS_V8EMBED=ON -DSUPPRESS_REMBED=ON
```

### Clean Rebuild

To clean and rebuild:

```bash
# Clean build files
cmake --build . --target clean

# Or remove build directory entirely
rm -rf build
mkdir build
cd build
# Re-run cmake configuration
```

### Getting Help

- **Documentation**: Check `devdoc/` directory for developer docs
- **Wiki**: https://github.com/hpcc-systems/HPCC-Platform/wiki
- **Issues**: https://github.com/hpcc-systems/HPCC-Platform/issues
- **Community**: HPCC Systems forums and mailing lists

## Advanced Configuration

### Custom Installation Prefix

```bash
cmake -B . -S .. -DCMAKE_INSTALL_PREFIX=/opt/hpcc
```

### Cross-compilation

For ARM targets:

```bash
cmake -B . -S .. \
  -DCMAKE_TOOLCHAIN_FILE=cmake/arm-toolchain.cmake \
  -DTARGET_ARCH=arm64
```

### Performance Tuning

For optimized builds:

```bash
cmake -B . -S .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS="-O3 -march=native"
```

---

This guide should get you started with building HPCC Platform from master. For specific development tasks, refer to the documentation in the `devdoc/` directory.