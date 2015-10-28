/*##############################################################################

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
############################################################################## */

You must have configured an instance of dynamicEsdl in this environment.  Either through configmgr or by editing environment.xml.  This document assumes dynamicESDL is running on "myesp" port 8088.

Run the following from the node running the eclwatch server:

Make a copy of the /opt/HPCCSystems/examples/EsdlExample folder.

From EsdlExample copy folder:

1. Generate java base classes (and dummy implementation file): 
esdl java esdl_example.esdl EsdlExample --version=9

2. Compile java base classes and example service (must have sudo access to place the classes in the default HPCC class location):
sudo javac -g EsdlExampleServiceBase.java -cp /opt/HPCCSystems/classes -d /opt/HPCCSystems/classes/
sudo javac -g EsdlExampleService.java -cp /opt/HPCCSystems/classes -d /opt/HPCCSystems/classes/

3. Generate ecl layouts:
esdl ecl esdl_example.esdl .

4. Publish example roxie query:
ecl publish roxie RoxieEchoPersonInfo.ecl

5. Publish the esdl defined service to dynamicESDL:
 esdl publish EsdlExample esdl_example.esdl --version 9 --overwrite

5. Bind both java and roxie implementations to DynamicESDL
esdl bind-service myesp 8088 esdlexample.1 EsdlExample --config esdl_binding.xml --overwrite

6. Test calling the java service:
soapplus -url http://.:8088/EsdlExample -i javarequest.xml

7. Test calling the roxie service:
soapplus -url http://.:8088/EsdlExample -i roxierequest.xml

8. Interact with both services by browsing to:

http://<DynamicEsdlIP>:8088

