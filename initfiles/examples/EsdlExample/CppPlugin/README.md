```
    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
```

# Overview

This folder has an example showing the use of an ESDL service implemented as a C++ plugin to the ESP.

These instructions were written and tested on a bare-metal deployment. Though the example may work in VM, containerized or cloud-based deployments, specific instructions for those variations aren't covered here.

# Environment Setup

This example requires some prior setup of your HPCC Systems deployment. It assumes you're running the stock installed configuration from `/etc/HPCCSystems/environment.xml` on a single machine with shell access. In addition you should have the prerequisites required to build HPCC, outlined at https://github.com/hpcc-systems/HPCC-Platform/wiki/Building-HPCC

## DynamicESDL

You must have configured at least one DynamicESDL ESP Service in this environment, either through configmgr or by editing environment.xml. The instance can be bound on port 0, or on the same port as where you're planning to run your service, or both. The purpose of the instance is to provide the configuration needed for your service. If there's an instance configured on the same port, that instance's configuration will be used, otherwise the configuration of the instance on port 0 will be used. This document assumes you're planning to bind your service to esp process "myesp" and port 8088.

## Source Code

Ensure your host machine has the required source code:

1. Copy of HPCCSystems source- example path is `/home/yourusername/src/hpccsystems`
2. Location of HPCCSystems build artifacts- example path is `/home/yourusername/builds/hpccsystems`

# C++ Example

Make a copy of the `/opt/HPCCSystems/examples/EsdlExample` folder onto the host that will run the example. Change to the newly copied `EsdlExample/CppPlugin` folder and run the following commands:

1. Generate C++ stub files and base class files, put into a folder named cpptest
```
esdl cpp esdl_example.esdl EsdlExample --outdir cpptest
```

2. Copy the pre-written implementation file over the generated stub in `cpptest/source`
```
cp EsdlExampleService.cpp cpptest/source
```

3. Change to build diretory
```
cd cpptest/build
```

4. Run cmake to create makefiles for the project
```
cmake ../source/ -DHPCC_SOURCE_DIR=/home/yourusername/src/hpccsystems -DHPCC_BUILD_DIR=/home/yourusername/builds/hpccsystems -DCMAKE_BUILD_TYPE=Debug -G "Eclipse CDT4 - Unix Makefiles"
```

5. Compile. The plugin shared object will be created in the `cpptest/build` directory
```
make -j8
```

6. Copy the plugin `libEsdlExampleService.so` to the *plugins* folder under your HPCC running directory. In a stock install the plugins folder is `/opt/HPCCSystems/plugins`

7. Publish the ESDL definition
```
esdl publish ../../esdl_example.esdl EsdlExample --overwrite
```

8. Bind service with the config similar to the following
```
esdl bind-service myesp 8088 esdlexample.1 EsdlExample --config ../../cpp_binding.xml --overwrite
```

9. Test calling the roxie service
```
soapplus -url http://.:8088/EsdlExample -i ../../cpprequest.xml
```

10. Interact with the services by browsing to `http://127.0.0.1:8088`
