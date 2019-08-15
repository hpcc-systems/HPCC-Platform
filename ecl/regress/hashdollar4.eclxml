<Archive useArchivePlugins="1">
<!--

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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
 <Module name="x1.y1">
  <Attribute name="m1">
export m1 := MODULE
    export m2() := MACRO

   import #$;
   import #$.^ as me;
   import myMod from #$.^;

   output(#$.value);
   output(me.value);
   output(myMod.value)
ENDMACRO;
  export value := 'x1.y1.m1.value';
END;
  </Attribute>
  <Attribute name="value">
export value := 'Good';
  </Attribute>
  <Attribute name="mymod">
export mymod := MODULE
    export value := 'Good II';
END;
  </Attribute>
 </Module>
 <Module name="x2">
  <Attribute name="f1">
  import x1;
  x1.y1.m1.m2();
  </Attribute>
  <Attribute name="value">
export value := 'Bad';
  </Attribute>
 </Module>
 <Query>
    import x1,x2;
    x2.f1;
 </Query>
</Archive>
