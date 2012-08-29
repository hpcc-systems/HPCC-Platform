<Archive legacyMode="1">
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
  <Attribute name="myAttr">
  EXPORT myAttr() := MACRO
      hello := 'hello';
      sp := ' ';
      world := 'world';
      //Check that legacy mode means that modules are imported by default.
      hello + sp + otherModule.myAttr
  ENDMACRO;
    </Attribute>
  </Module>
    <Module name="otherModule">
        <Attribute name="myAttr">
            EXPORT myAttr := 'world';
        </Attribute>
    </Module>
    <Query attributePath="myModule.myAttr"/>
</Archive>
