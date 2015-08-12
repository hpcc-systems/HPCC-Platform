<Archive>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
 <Module name="ghalliday.nested">
  <Attribute name="gh1">
export gh1 := gh2 * 'x';
  </Attribute>
  <Attribute name="gh2">
export gh2 := 100;
  </Attribute>
  <Attribute name="gh3">
shared gh3 := 200;
  </Attribute>
 </Module>
 <SyntaxCheck module="ghalliday.nested" attribute="gh1">
//Check no need to import, definition is overridden, and shared values accessed correctly
export gh1 := gh2 * 99;
 </SyntaxCheck>>
</Archive>
