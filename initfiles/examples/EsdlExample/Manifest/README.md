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

This example shows how a binding for a complex service is built from separate source files using a manifest file and the `esdl manifest` command. The service uses a roxie implementation combined with the ESDL Scripts.

These instructions were written and tested on a bare-metal deployment. Though the example may work in VM, containerized or cloud-based deployments, specific instructions for those variations aren't covered here.

# Environment Setup

This example requires some prior setup of your HPCC Systems deployment. It assumes you're running the stock installed configuration from `/etc/HPCCSystems/environment.xml` on a single machine with shell access.

## DynamicESDL Configuration

You must have configured at least one DynamicESDL ESP Service in this environment, either through configmgr or by editing environment.xml. The instance can be bound on port 0, or on the same port as where you're planning to run your service, or both. The purpose of the instance is to provide the configuration needed for your service. If there's an instance configured on the same port, that instance's configuration will be used, otherwise the configuration of the instance on port 0 will be used. This document assumes you're planning to bind your service to esp process "myesp" and port 8088.

# Manifest Example

Make a copy of the `/opt/HPCCSystems/examples/EsdlExample` folder onto the host that will run the example. Change to the newly copied `EsdlExmple/Manifest` folder and run the following commands:

1. Generate ecl layouts
```
esdl ecl esdl_example.esdl .
```

2. Publish example roxie query
```
ecl publish roxie RoxieEchoPersonInfo.ecl
```

3. Publish the esdl defined service to dynamicESDL
```
esdl publish esdl_example.esdl EsdlExample --overwrite
```

4. Create the binding from a manifest file
```
esdl manifest manifest_example.xml -I EsdlScripts --outfile manifest_example_binding.xml
```

5. Bind roxie backend implementation to DynamicESDL
```
esdl bind-service myesp 8088 esdlexample.1 EsdlExample --config manifest_example_binding.xml --overwrite
```

1. Test calling the service with the roxie backend and ESDL Scripts
```
soapplus -url http://.:8088/EsdlExample -i roxierequest.xml
```

7. Interact with the service by browsing to `http://127.0.0.1:8088`
