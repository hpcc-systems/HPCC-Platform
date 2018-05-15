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

You must have configured at least one instance of dynamicESDL in this environment.  Either through configmgr or by editing environment.xml. The instance can be bound on port 0, or on the same port as where you're planning to run your service, or both. The purpose of the instance is to provide the configuration needed for your service. If there's an instance configured on the same port, that instance's configuration will be used, otherwise the configuration of the instance on port 0 will be used. This document assumes you're planning to bind your service to esp process "myesp" and port 8088.

Run the following from the node running the eclwatch server:

Make a copy of the /opt/HPCCSystems/examples/EsdlExample folder.

From EsdlExample copy folder:

1. Generate java base classes (and dummy implementation file): 
esdl java esdl_example.esdl EsdlExample

2. Compile java base classes and example service (must have sudo access to place the classes in the default HPCC class location):
sudo javac -g EsdlExampleServiceBase.java -cp /opt/HPCCSystems/classes -d /opt/HPCCSystems/classes/
sudo javac -g EsdlExampleService.java -cp /opt/HPCCSystems/classes -d /opt/HPCCSystems/classes/

3. Generate ecl layouts:
esdl ecl esdl_example.esdl .

4. Publish example roxie query:
ecl publish roxie RoxieEchoPersonInfo.ecl

5. Publish the esdl defined service to dynamicESDL:
esdl publish esdl_example.esdl EsdlExample --overwrite

5. Bind both java and roxie implementations to DynamicESDL
esdl bind-service myesp 8088 esdlexample.1 EsdlExample --config esdl_binding.xml --overwrite

6. Test calling the java service:
soapplus -url http://.:8088/EsdlExample -i javarequest.xml

7. Test calling the roxie service:
soapplus -url http://.:8088/EsdlExample -i roxierequest.xml

8. Interact with both services by browsing to:

http://<DynamicEsdlIP>:8088

9. To debug the java service:
Uncomment the jvmoptions line in environment.conf:
jvmoptions=-XX:-UsePerfData -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=2000
And comment out the original jvmoptions line:
#jvmoptions=-XX:-UsePerfData
From eclipse for java, create a new debug configuration for remote java application, under connect tab, select your esdl java service project, select standard socket attach connection type, fill in the host and port. The port should match what you've specified in the jvmoptions inside environment.conf, which is 2000 in our example above. Click the Debug button and off you go.
