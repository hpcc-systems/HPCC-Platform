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
  <Attribute name="attribute_a">
export attribute_A := MODULE
  export layout := RECORD
    unsigned did;
    attribute_B.counts;
  end;

  export ds := dataset ([], layout);
END;
  </Attribute>
  <Attribute name="attribute_b">
    import common;
export Attribute_B := MODULE
  export counts := RECORD
    unsigned attribute_a := 0;
    unsigned other := 0;
  end;
END;  </Attribute>
  <Attribute name="x">
export x := 100;
  </Attribute>
 </Module>
 <Query>
    import common;
    r := common.attribute_b;
 output(common.attribute_a.ds);
 </Query>
</Archive>
