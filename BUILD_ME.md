```
HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 

http://hpccsystems.com
``` 

To build for Linux:
-------------------

Prerequisites: (w/ base Ubuntu 14)
```
sudo apt-get install cmake
sudo apt-get install bison
sudo apt-get install flex
sudo apt-get install binutils-dev
sudo apt-get install libiberty-dev
sudo apt-get install slapd
sudo apt-get install openldap-dev
sudo apt-get install libicu-dev
sudo apt-get install libxslt-dev
sudo apt-get install zlib1g-dev
sudo apt-get install libarchive-dev
sudo apt-get install libboost-all-dev
sudo apt-get install libssl-dev
sudo apt-get install libapr1-dev
sudo apt-get install libaprutil1-dev
sudo apt-get install clang
sudo apt-get install libtool
sudo apt-get install autotools-dev
sudo apt-get install automake
```

* Check out sources (for example, to directory ~/hpcc)
* Fetch all sub-modules with:

```
   git submodule update --init --recursive
```   
* Create a build directory - either as a child of hpcc or elsewhere
* cd to the build directory
* To create makefiles to build native release version for local machine, run
```
   cmake ~/hpcc
```
* To create makefiles to build native debug version, run
```
   cmake -DCMAKE_BUILD_TYPE=Debug ~/hpcc
```
*  To create makefiles to build 32-bit version from 64-bit host, run
```
   cmake -DCMAKE_C_FLAGS:STRING="-m32 -march=i386" -DCMAKE_CXX_FLAGS:STRING="-m32 -march=i386" ~/hpcc
```
* To build the makefiles just created above, run
```
   make
```
* Executables will be created in ./&lt;releasemode&gt;/bin and ./&lt;releasemode&gt;/libs
* To create a .deb / ,rpm to install, run
```
   make package
```

* To install from the built binaries, you will need node.js installed. 
(Be sure to run both of the lines below!  See: https://github.com/joyent/node/wiki/Installing-Node.js-via-package-manager)
``` 
   curl -sL https://deb.nodesource.com/setup | sudo bash -
   sudo apt-get install -y nodejs
```

* Then, you can run make install as root.
```
   sudo make install
```

 
To build for Windows:
---------------------

1. Check out sources (for example, to directory c:\hpcc)
2. Create a build directory - either as a child of hpcc or elsewhere
3. cd to the build directory
4. To create a Visual Studio project, run
```
   cmake c:\hpcc -G "Visual Studio 9 2008"
```
5. The sln file hpccsystems-platform.sln will be created in the current directory, and will support debug and release targets
6. To build the project, load the solution into the visual studio IDE and build in the usual way.
7. Executables will be created in (for example) c:\hpcc\bin\&lt;releasemode&gt;

To build client tools for Macintosh OSX:
----------------------------------------

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
   brew install bison27 
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
   cmake ../ -DICU_LIBRARIES=/usr/local/opt/icu4c/lib/libicuuc.dylib -DICU_INCLUDE_DIR=/usr/local/opt/icu4c/include -DLIBARCHIVE_INCLUDE_DIR=/usr/local/opt/libarchive/include -DLIBARCHIVE_LIBRARIES=/usr/local/opt/libarchive/lib/libarchive.dylib -DBOOST_REGEX_LIBRARIES=/usr/local/opt/boost/lib -DBOOST_REGEX_INCLUDE_DIR=/usr/local/opt/boost/include -DCLIENTTOOLS_ONLY=true -DUSE_OPENLDAP=true -DOPENLDAP_INCLUDE_DIR=/usr/local/opt/openldap/include -DOPENLDAP_LIBRARIES=/usr/local/opt/openldap/lib/libldap_r.dylib
```

* To build the makefiles just created above, run
   make
* Executables will be created in ./&lt;releasemode&gt;/bin and ./&lt;releasemode&gt;/libs
* To create a .dmg to install, run
   make package
 
 
