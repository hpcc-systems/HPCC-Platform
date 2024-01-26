```
    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

This folder has several examples using ESDL to create services, each in a subdirectory. Check the README.md file in each directory for example details and directions. Currently these examples are tailored for a bare-metal deployments, with container or cloud focused examples forthcoming. A brief description of each example is below:

| Example        | Description                                        |
-----------------|----------------------------------------------------|
| BasicRoxie     | A service running on Roxie customized with some simple ESDL Scripts. |
| CppPlugin      | A service running as a C++ plugin. |
| JavaPlugin     | A service running as a Java plugin. |
| Manifest       | A service running on Roxie customized with complex ESDL Scripts. The service configuration is built from a manifest file and separately maintained files in the EsdlScripts folder. This is to show how you can use configuration as code by managing complex service scripts in a code respository along with a manifest file to derive the final service configuration.|
| MySqlService   | A work in progress that will show how you can use a combination of ESDL Scripts and a backing MySql database to implment a service.
