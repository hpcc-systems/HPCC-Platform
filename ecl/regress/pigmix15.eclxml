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

<Archive ignoreUnknownImport="1">
 <Module name="pigtypes">
  <Attribute name="notype">
   export notype := string;
  </Attribute>
  <Attribute name="_group">
   export _GROUP(InFile,Fld,InFile_Label,outf) := MACRO
#uniquename(r)
%r% := RECORD,MAXLENGTH(6400000)
    Group_Key := InFile.Fld;
    InFile_Label := DATASET(ROW(InFile));
  END;

#uniquename(agg)
%r% %agg%(%r% le,%r% ri) := TRANSFORM
  SELF.Group_Key := le.Group_Key;
    SELF.InFile_Label := le.InFile_Label+ri.InFile_label;
  END;

outf := ROLLUP( SORT( DISTRIBUTE( table(InFile,%r%), HASH(TRIM((STRING)Group_Key))), Group_Key, LOCAL ), LEFT.Group_Key=RIGHT.Group_Key, %agg%(left,right), LOCAL );

  ENDMACRO;
  </Attribute>
 </Module>
 <Query>//BACON V0.0.10 Alpha generated ECL
IMPORT PigTypes,PigMix;
// $identifer appearing inside string scalars can be mapped onto the ECL STORED capability
STRING Param_out := &apos;&apos; : STORED(&apos;out&apos;);
a := DATASET(&apos;page_views&apos;,{PigTypes.NoType user,PigTypes.NoType action,PigTypes.NoType timespent,PigTypes.NoType query_term,PigTypes.NoType ip_addr,PigTypes.NoType timestamp,unsigned8 estimated_revenue,PigTypes.NoType page_info,PigTypes.NoType page_links},CSV(SEPARATOR(&apos;\t&apos;)));
b := TABLE(a,{user,estimated_revenue});
PigTypes._GROUP(b,user,b_records,group_out0_0);
c := group_out0_0;
d := TABLE(c,{Group_Key,SUM(TABLE(SORT(b_records,estimated_revenue),{estimated_revenue}),estimated_revenue)});
OUTPUT(d,,Param_out);&#13;&#10;&#13;&#10;&#13;&#10;</Query>
</Archive>
