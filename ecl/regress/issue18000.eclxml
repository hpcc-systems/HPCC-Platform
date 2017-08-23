<Archive build="community_6.4.0-rc6Debug"
         eclVersion="6.4.0"
         legacyImport="0"
         legacyWhen="0">
 <Query attributePath="_local_directory_.issue18000"/>
 <Option name="syntaxCheck" value="1"/>
 <Module key="_local_directory_" name="_local_directory_">
  <Attribute key="issue18000" name="issue18000" sourcePath="issue18000.ecl">
   /*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */


export issue18000(AttrName,FirstArg,SecondArg) := MACRO
    AttrName := FirstArg + SecondArg;
ENDMACRO;
  </Attribute>
 </Module>
</Archive>
