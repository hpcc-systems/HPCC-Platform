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
 <Module name="mal_temp">
  <Attribute name="library_impl">
   export library_impl(String1 ba=&apos;&apos;) := module,library(mal_temp.library_interface)
            export string business_code := ba;
    ENd;
  </Attribute>
  <Attribute name="library_interface">
   export library_interface(String1 ba) := INTERFACE

            export String business_code;

        END;
  </Attribute>
 </Module>
 <Query>
    import mal_temp;
    x:= mal_temp.library_impl;&#13;&#10;&#13;&#10;&#13;&#10;
 build(x);
 </Query>
</Archive>
