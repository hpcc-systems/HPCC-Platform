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
 <Module name="x1">
  <Attribute name="m1">
     import x2,x3;
export m1 := x1.m2+x2.m1;
  </Attribute>
    <Attribute name="m2">
       export m2 := 88;
    </Attribute>
 </Module>
   <Module name="x2">
      <Attribute name="m1">
         export m1 := 123;
      </Attribute>
   </Module>
   <Module name="x3">
      <Attribute name="m1">
         export m1 := 456;
      </Attribute>
   </Module>
 <Query>
    import m1 from x1 as x1;
 output(x1);
 </Query>
</Archive>
