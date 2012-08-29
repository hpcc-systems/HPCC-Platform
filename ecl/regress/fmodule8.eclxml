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
 <Module name="common">
  <Attribute name="m1">
export m1 := module,forward
export unsigned level1 := SyntaxError;
//unsigned scale := level1;
export unsigned level2 := common.m2.levela * level1;
export unsigned level3(unsigned i) := common.m2.levelc(i) * level1;
    end;
  </Attribute>
  <Attribute name="m2">
export m2 := module,forward
export unsigned levela := 10;
export unsigned levelb := m1.level1 * levela;
export unsigned levelc(unsigned i) := i * (common.m1.level2 +123);
    end;
  </Attribute>
  <Attribute name="x">
export x := 100;
  </Attribute>
 </Module>
 <Query>
    import common;
 output(common.m1.level1);
 output(common.m1.level1);
 output(common.m1.level2);
 output(common.m1.level3(99));
 </Query>
</Archive>
