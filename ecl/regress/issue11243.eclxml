<Archive useArchivePlugins="1">
<!--

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
-->
 <Module name="myModule">
  <Attribute name="applyLimits">
r := { unsigned id := 0; };
export applyLimits := normalize(dataset(row(r)), 1000, transform(r, self.id := counter));
  </Attribute>
 </Module>
 <Option name="applyInstantEclTransformations" value="1"/>
 <Option name="applyInstantEclTransformationsLimit" value="123"/>
 <Query attributePath=".applyLimits"/>
</Archive>
