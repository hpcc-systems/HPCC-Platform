<Archive useArchivePlugins="1">
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
 <Module name="common">
  <Attribute name="m1">
export m1(unsigned k) := module,forward
export unsigned level1 := k*10;
export unsigned level2 := common.m2.level1 + level1;
export unsigned level3(unsigned i) := common.m2.level3(i) / level1;
    end;
  </Attribute>
  <Attribute name="m2">
export m2 := module,forward
export unsigned level1 := 10;
export unsigned level2 := 22;//common.m1(9).level1 * level1;
export unsigned level3(unsigned i) := i;// * (common.m1(6).level2 +123);
    end;
  </Attribute>
 </Module>
 <Query>
    import common;
 m := common.m1(15);
 output(m.level1);
 output(m.level2);
 output(m.level3(99));
 </Query>
</Archive>
