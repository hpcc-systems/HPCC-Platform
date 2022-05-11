# Building with VCPKG

_[vcpkg](https://github.com/microsoft/vcpkg) is a C++ Library Manager for Windows, Linux, and MacOS.  The primary motivation for integration into the HPCC-Platform build pipeline is to be able to build the platform / client tools without the need of any "secret sauce" or random downloads to third party libraries, especially when building on Windows and OSX._

**Note:** _This is a work in progress and is not yet complete and the expectation is that the instructions will get simplified over time._

## Prerequisits 

* git
* cmake
* Build tools (gcc, clang, Visual Studio, NSIS etc.)

## Steps

### 1 - Clone the repository

```sh
git clone https://github.com/hpcc-systems/HPCC-Platform.git HPCC-Platform
git checkout candidate-8.6.x
cd HPCC-Platform
git submodule update --init --recursive
```

### Create an out of source build folder
```
mkdir build
cd build
```

### Generate build configurations

#### Linux

```sh
cmake .. -DRUNTIME_USER=$(whoami) -DRUNTIME_GROUP=$(id -gn) -DDESTDIR=$(realpath ../..)/opt -DCMAKE_TOOLCHAIN_FILE=../vcpkg/scripts/buildsystems/vcpkg.cmake -DVCPKG_OVERLAY_PORTS=../vcpkg-overlays -DCMAKE_BUILD_TYPE=Debug -DUSE_OPTIONAL=OFF -DINCLUDE_PLUGINS=ON -DSUPPRESS_V8EMBED=ON -DSKIP_ECLWATCH=ON
cmake --build . -- -j
```

#### OSX 

```sh
cmake .. -DCMAKE_TOOLCHAIN_FILE=../vcpkg/scripts/buildsystems/vcpkg.cmake -DX_VCPKG_APPLOCAL_DEPS_INSTALL=ON -DVCPKG_OVERLAY_PORTS=../vcpkg-overlays -DCMAKE_BUILD_TYPE=Debug -DUSE_OPTIONAL=OFF -DINCLUDE_PLUGINS=OFF -DUSE_OPENLDAP=OFF -DUSE_AZURE=OFF -DUSE_AWS=OFF -DWSSQL_SERVICE=OFF -DUSE_CASSANDRA=OFF  -DSKIP_ECLWATCH=ON
cmake --build . -- -j
```

#### Windows (VS 2019 x64) 

```sh
cmake .. -G "Visual Studio 16 2019" -T host=x64 -A x64 -DCMAKE_TOOLCHAIN_FILE=../vcpkg/scripts/buildsystems/vcpkg.cmake -DX_VCPKG_APPLOCAL_DEPS_INSTALL=ON -DVCPKG_OVERLAY_PORTS=../vcpkg-overlays -DUSE_OPTIONAL=OFF -DUSE_NATIVE_LIBRARIES=ON -DINCLUDE_PLUGINS=OFF -DUSE_OPENLDAP=OFF -DUSE_AZURE=OFF -DUSE_AWS=OFF -DWSSQL_SERVICE=OFF -DUSE_CASSANDRA=OFF -DSKIP_ECLWATCH=ON
cmake --build . --config Debug -- -m
```

### Create Installer

#### OSX / Linux

```sh
cmake --build . --target package -- -j
```

#### Windows (VS 2019 x64) 

```sh
cmake --build . --config Release --target package -- -m
```
