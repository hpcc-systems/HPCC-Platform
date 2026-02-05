import{_ as s,a as i,o as t,ag as l}from"./chunks/framework.Do1Zayaf.js";const h=JSON.parse('{"title":"","description":"","frontmatter":{},"headers":[],"relativePath":"helm/examples/esdl-sandbox/README.md","filePath":"helm/examples/esdl-sandbox/README.md","lastUpdated":1770295448000}'),n={name:"helm/examples/esdl-sandbox/README.md"};function a(o,e,p,r,d,c){return t(),i("div",null,e[0]||(e[0]=[l(`<p>/*##############################################################################</p><pre><code>HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
</code></pre><p>############################################################################## */</p><p>Example of esdl-sandbox service. This service is an alpha release. It may change quite a bit before the final release.</p><p>After deploying a helm k8s hpcc..</p><p>From helm/examples/esdl-sandbox folder:</p><ol><li><p>Generate ecl layouts: esdl ecl esdl_example.esdl .</p></li><li><p>Publish example roxie query: ecl publish roxie RoxieEchoPersonInfo.ecl</p></li><li><p>Publish the esdl defined service to dynamicESDL: esdl publish esdl_example.esdl EsdlExample</p></li><li><p>Bind both java and roxie implementations to DynamicESDL (note that here, port is internal pod port not service port) esdl bind-service esdl-sandbox 8880 esdlexample.1 EsdlExample --config esdl_binding.xml</p></li><li><p>Test calling the service: soapplus -url http://.:8899/EsdlExample -i roxierequest.xml</p></li><li><p>Interact with both services by browsing to:</p></li></ol><p>http://&lt;DynamicEsdlIP&gt;:8899</p>`,8)]))}const x=s(n,[["render",a]]);export{h as __pageData,x as default};
