/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

From EsdlExample copy folder:

1. Generate c++ files (which will be put under cpptest folder):
esdl cpp esdl_example.esdl EsdlExample --outdir cpptest

2. Change to build diretory
cd cpptest/build

3. Run cmake
cmake ../source/ -DHPCC_SOURCE_DIR=/home/mayx/git/HPCC-Platform -DHPCC_BUILD_DIR=/home/mayx/git/ecbuild -DCMAK
E_BUILD_TYPE=Debug -G"Eclipse CDT4 - Unix Makefiles"

4. Compile, the plugin shared object will be created
make -j8

5. Copy the plugin SO to the "plugins" folder under your HPCC running directory

6. Bind service with the config similar to the following
    <Method name="CppEchoPersonInfo" querytype="cpp" method="onEsdlExampleCppEchoPersonInfo" plugin="libEsdlExampleService.so"/>
