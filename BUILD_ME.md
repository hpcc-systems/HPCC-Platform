```
HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

https://hpccsystems.com
```

# Build Instructions

* Visit [https://github.com/hpcc-systems/HPCC-Platform/wiki](https://github.com/hpcc-systems/HPCC-Platform/wiki) for up-to-date help and guidance.

## Prerequisites

### Ubuntu
#### Ubuntu 19.10/19.04/18.04

    sudo apt-get install cmake bison flex build-essential binutils-dev libldap2-dev libcppunit-dev libicu-dev libxslt1-dev \
    zlib1g-dev libboost-regex-dev libarchive-dev python-dev libv8-dev default-jdk libapr1-dev libaprutil1-dev libiberty-dev \
    libhiredis-dev libtbb-dev libxalan-c-dev libnuma-dev nodejs libevent-dev libatlas-base-dev libblas-dev python3-dev \
    default-libmysqlclient-dev libsqlite3-dev r-base-dev r-cran-rcpp r-cran-rinside r-cran-inline libmemcached-dev \
    libcurl4-openssl-dev pkg-config libtool autotools-dev automake libssl-dev

#### Ubuntu 16.04

    sudo apt-get install cmake bison flex build-essential binutils-dev libldap2-dev libcppunit-dev libicu-dev libxslt1-dev \
    zlib1g-dev libboost-regex-dev libssl-dev libarchive-dev python-dev libv8-dev default-jdk libapr1-dev libaprutil1-dev \
    libiberty-dev libhiredis-dev libtbb-dev libxalan-c-dev libnuma-dev libevent-dev libatlas-base-dev libblas-dev \
    libatlas-dev python3-dev libcurl4-openssl-dev libtool autotools-dev automake

##### Additional information for building versions prior to 7.0 on Ubuntu 18.04 #####

Get openssl-1.0.2o.tar.gz from https://www.openssl.org/source/

unpack, build, and install:

    ./config -fPIC shared
    make
    make install

This installs to /usr/local/ssl by default. 

Build the platform with the following additional CMake options:

    -DOPENSSL_LIBRARIES=/usr/local/ssl/lib/libssl.so -DOPENSSL_SSL_LIBRARY=/usr/local/ssl/lib/libssl.so

### CentOS

Regardless of which version of CentOS you will be building on, it is suggested that you enable the EPEL repository 

```bash
sudo yum install -y epel-release
```

#### CentOS 7:

    sudo yum install gcc-c++ gcc make bison flex binutils-devel openldap-devel libicu-devel libxslt-devel libarchive-devel \
    boost-devel openssl-devel apr-devel apr-util-devel hiredis-devel numactl-devel mariadb-devel libevent-devel tbb-devel \
    atlas-devel python34 libmemcached-devel sqlite-devel v8-devel python-devel python34-devel java-1.8.0-openjdk-devel \
    R-core-devel R-Rcpp-devel R-inline R-RInside-devel nodejs cmake3 rpm-build libcurl-devel

#### CentOS 6.4:

    sudo yum install gcc-c++ gcc make bison flex binutils-devel openldap-devel libicu-devel libxslt-devel libarchive-devel \
    boost-devel openssl-devel apr-devel apr-util-devel hiredis-devel numactl-devel libmysqlclient-dev libevent-devel \
    tbb-devel atlas-devel python34 R-core-devel R-Rcpp-devel R-inline R-RInside-devel nodejs libcurl-devel

### Other Platforms
#### Fedora 19:

    sudo yum install gcc-c++ gcc make fedora-packager cmake bison flex binutils-devel openldap-devel libicu-devel  \
    xerces-c-devel xalan-c-devel libarchive-devel boost-devel openssl-devel apr-devel apr-util-devel

#### Fedora 23:

    sudo dnf install gcc-c++ gcc make fedora-packager cmake bison flex binutils-devel openldap-devel libicu-devel \
    xerces-c-devel xalan-c-devel libarchive-devel boost-devel openssl-devel apr-devel apr-util-devel numactl-devel \
    tbb-devel libxslt-devel nodejs

#### Mac (Snow Leopard):

    sudo port install bison flex binutils openldap icu xalanc zlib boost openssl libarchive

You can disable some functionality in order to reduce the list of required components, if necessary. Optional components, such as the plugins for interfacing to external languages, will be disabled automatically if the required libraries and headers are not found at build time.

Optional dependencies:

To build with support for all the plugins for third party embedded code, additional dependencies may be required. If not found, the system will default to skipping those components.

#### Windows

From 7.4 the build system has been changed to make it easier to build on windows.  It is dependent on two other projects chocolatey and vcpkg for installing the dependencies:

Install chocolatey:
```
https://chocolatey.org/install
```

Use that to install bison/flex:

```
choco install winflexbison3
```

Install vcpkg:

```
git clone https://github.com/Microsoft/vcpkg
cd vcpkg
bootstrap-vcpkg
```

Use vcpkg to install various packages:
```
vcpkg install zlib
vcpkg install boost
vcpkg install icu
vcpkg install libxslt
vcpkg install tbb
vcpkg install cppunit
vcpkg install libarchive
vcpkg install apr
vcpkg install apr-util
```
You may need to force vcpkg to build the correct version, e.g. for 64bit:
```
vcpkg install zlib zlib:x64-windows
```

### Other required third-party packages
#### Nodejs 
NodeJS (version 8.x.x LTS recommended) is used to package ECL Watch and related web pages. 

To install nodeJs on Linux based systems, try:

```
   curl -sL https://deb.nodesource.com/setup_8.x | sudo bash -
   sudo apt-get install -y nodejs
```

If these instructions do not work on your system, refer to the detailed instructions available [here](https://nodejs.org/en/download/package-manager/)


### Building with R Support:

  First insure that the R language is installed on your system.  For Ubuntu use `sudo apt-get install r-base-dev`.  For centos distributions use `sudo yum install -y R-core-devel`.

  To install the prerequisites for building R support, use the following for all distros:
```
wget https://cran.r-project.org/src/contrib/00Archive/Rcpp/Rcpp_0.12.1.tar.gz
wget https://cran.r-project.org/src/contrib/00Archive/RInside/RInside_0.2.12.tar.gz
wget http://cran.r-project.org/src/contrib/inline_0.3.14.tar.gz
sudo R CMD INSTALL Rcpp_0.12.1.tar.gz RInside_0.2.12.tar.gz inline_0.3.14.tar.gz
```


## Get Latest HPCC Systems Sources

Visit [Git-step-by-step](https://github.com/hpcc-systems/HPCC-Platform/wiki/Git-step-by-step) for full instructions.

To get started quickly, simply:

```bash
git clone [-b <branch name>] --recurse-submodules https://github.com/hpcc-systems/HPCC-Platform.git
```

Where [ ] denotes an optional argument.


## CMake

The minimum version of CMake required to build the HPCC Platform is 3.3.2 on Linux.  You may need to download a recent version [here at cmake.org](https://cmake.org/download/).

Now you need to run CMake to populate your build directory with Makefiles and special configuration to build HPCC, packages, tests, etc.

A separate directory is required for the build files. In the examples below, the source directory is contained in ~/hpcc and the build directory is ~/hpcc/build.
    mkdir ~/hpcc/build

All cmake commands would normally need to be executed within the build directory:
    cd ~/hpcc/build

For release builds, do:
    cmake ../src

To enable a specific plugin in the build:
```bash
    cmake –D<Plugin Name>=ON ../src 
    make –j6 package
```

These are the current supported plugins: 
* CASSANDRAEMBED
* REMBED
* V8EMBED
* MEMCACHED
* PYEMBED
* REDIS
* JAVAEMBED
* KAFKA
* SQLITE3EMBED
* MYSQLEMBED

If testing during development and you may want to include plugins (except R) in the package:
    cmake -DTEST_PLUGINS=ON ../src
  
To produce a debug build:
    cmake -DCMAKE_BUILD_TYPE:STRING=Debug ../src

To build the client tools only:
    cmake -DCLIENTTOOLS_ONLY=1 ../src

To enable signing of the ecl standard library, ensure you have a gpg private key loaded into your gpg keychain and do:
    # Add -DSIGN_MODULES_KEYID and -DSIGN_MODULES_PASSPHRASE if applicable
    cmake -DSIGN_MODULES=ON ../src

In some cases, users have found that when packaging for Ubuntu, the dpkg-shlibdeps portion of the packaging adds an exceptional amount of time to the build process.  To turn this off (and to create a package without dynamic dependency generation) do:
    cmake -DUSE_SHLIBDEPS=OFF ../src

CMake will check for necessary dependencies like binutils, boost regex, cppunit, pthreads, etc. If everything is correct, it'll create the necessary Makefiles and you're ready to build HPCC.

#### NOTE:
We default to using libxslt in place of Xalan for xslt support. Should you prefer to use libxalan, you can specify -DUSE_LIBXALAN on the cmake command line.

## Building

You may build by either using make:

    # Using -j option here to specify 6 compilation threads (suitable for quad core cpu)
    make -j6

Or, alternatively you can call a build system agnostic variant (works with make, ninja, XCode, Visual Studio etc.):

    cmake --build .

This will make all binaries, libraries and scripts necessary to create the package.

* Executables will be created in ./&lt;releasemode&gt;/bin and ./&lt;releasemode&gt;/libs

## Creating a package

The recommended method to install HPCC Systems on your machine (even for testing) is to use distro packages. CMake has already detected your system, so it know whether to generate TGZ files, DEB or RPM packages.

Just type:

    make package

Alternatively you can use the build system agnostic variant:

    cmake --build . --target package

and it will create the appropriate package for you to install. The package file will be created inside the build directory.

## Installing the package

Install the package:

    sudo dpkg -i hpccsystems-platform-community_6.0.0-trunk0trusty_amd64.deb

(note that the name of the package you have just built will depend on the branch you checked out, the distro, and other options).

*Hint: missing dependencies may be fixed with:*

    sudo apt-get -f install

(see [here](https://help.ubuntu.com/community/InstallingSoftware) for Ubuntu based installation).


## To build client tools for MacOS:

*Note: These instructions may not be up to date*

* Check out sources (for example, to directory ~/hpcc)
* Fetch all sub-modules with:

```
   git submodule update --init --recursive
```
* You many need to install some 3rd-party dev packages using macports or brew. (brew installs shown below)

```
   brew install icu4c
   brew install boost
   brew install libarchive
   brew install bison
   brew install openldap
```

** Also make sure that bison is ahead of the system bison on your path.
`bison --version`
(The result should be > 2.4.1 )

** OS X has LDAP installed, but when compiling against it (/System/Library/Frameworks/LDAP.framework/Headers/ldap.h) you will get a `#include nested too deeply`, which is why you should install openldap.

* Create a build directory - either as a child of hpcc or elsewhere
* cd to the build directory
* Use clang to build the clienttools (gcc4.2 cores when compiling some of the sources):

```
   export CC=/usr/bin/clang
   export CXX=/usr/bin/clang++
   cmake ../ -DICU_LIBRARIES=/usr/local/opt/icu4c/lib/libicuuc.dylib -DICU_INCLUDE_DIR=/usr/local/opt/icu4c/include \
   -DLIBARCHIVE_INCLUDE_DIR=/usr/local/opt/libarchive/include \
   -DLIBARCHIVE_LIBRARIES=/usr/local/opt/libarchive/lib/libarchive.dylib \
   -DBOOST_REGEX_LIBRARIES=/usr/local/opt/boost/lib -DBOOST_REGEX_INCLUDE_DIR=/usr/local/opt/boost/include \
   -DCLIENTTOOLS_ONLY=true \
   -DUSE_OPENLDAP=true -DOPENLDAP_INCLUDE_DIR=/usr/local/opt/openldap/include \
   -DOPENLDAP_LIBRARIES=/usr/local/opt/openldap/lib/libldap_r.dylib
```

* To build the makefiles just created above, run
   make
* Executables will be created in ./&lt;releasemode&gt;/bin and ./&lt;releasemode&gt;/libs
* To create a .dmg to install, run
   make package


