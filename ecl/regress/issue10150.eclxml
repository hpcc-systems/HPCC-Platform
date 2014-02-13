<Archive>
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
  <Attribute name="v1">
export v1 := 'Hello';
  </Attribute>
 </Module>
 <Module name="a.b.c">
  <Attribute name="x">
export x := 3;
  </Attribute>
  <Attribute name="y">
export y := 5;
  </Attribute>
  <Attribute name="z">
import $ as common;
export z := common.x * common.y;
  </Attribute>
 </Module>
 <Module name="a.b.d">
  <Attribute name="x">
export x := 7;
  </Attribute>
  <Attribute name="y">
export y := 11;
  </Attribute>
  <Attribute name="z">
import $ as common;
import $.^ as parent;
export z := common.x * parent.c.y;
  </Attribute>
 </Module>
 <Query>

import common;
import a.b.c as m1;
import a.b.d as m2;
output(common.v1);
output(m1.z);
output(m2.z);

 </Query>
</Archive>
