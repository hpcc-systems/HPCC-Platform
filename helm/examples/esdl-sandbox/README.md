/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

Example of esdl-sandbox service.  This service is an alpha release.  It may change quite a bit before the final release.

After deploying a helm k8s hpcc..

From helm/examples/esdl-sandbox folder:

1. Generate ecl layouts:
esdl ecl esdl_example.esdl .

2. Publish example roxie query:
ecl publish roxie RoxieEchoPersonInfo.ecl

3. Publish the esdl defined service to dynamicESDL:
esdl publish esdl_example.esdl EsdlExample

4. Bind both java and roxie implementations to DynamicESDL (note that here, port is internal pod port not service port)
esdl bind-service esdl-sandbox 8880 esdlexample.1 EsdlExample --config esdl_binding.xml

5. Test calling the service:
soapplus -url http://.:8899/EsdlExample -i roxierequest.xml

6. Interact with both services by browsing to:

http://<DynamicEsdlIP>:8899
