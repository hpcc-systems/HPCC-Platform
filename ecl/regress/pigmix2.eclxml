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

<Archive>
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

outf := ROLLUP( SORT( table(InFile,%r%), Group_Key ), LEFT.Group_Key=RIGHT.Group_Key, %agg%(left,right) );

  ENDMACRO;
  </Attribute>
 </Module>
 <Query>//BACON V0.0.9 Alpha generated ECL
IMPORT PigTypes;
// $identifer appearing inside string scalars can be mapped onto the ECL STORED capability
STRING Param_page_views := &apos;&apos; : STORED(&apos;page_views&apos;);
STRING Param_users := &apos;&apos; : STORED(&apos;users&apos;);
STRING Param_out := &apos;&apos; : STORED(&apos;out&apos;);
a := DATASET(Param_page_views,{PigTypes.NoType user,PigTypes.NoType action,PigTypes.NoType timespent,PigTypes.NoType query_term,PigTypes.NoType ip_addr,PigTypes.NoType timestamp,PigTypes.NoType estimated_revenue,PigTypes.NoType page_info,PigTypes.NoType page_links},CSV(SEPARATOR(&apos;\t&apos;)));
b := TABLE(a,{user});
alpha := DATASET(Param_users,{PigTypes.NoType name,PigTypes.NoType phone,PigTypes.NoType address,PigTypes.NoType city,PigTypes.NoType state,PigTypes.NoType zip},CSV(SEPARATOR(&apos;\t&apos;)));
beta := TABLE(alpha,{name});
PigTypes._GROUP(beta,name,beta_records,group_out0_0);
PigTypes._GROUP(a,user,a_records,group_out0_1);
join_out0_1 := JOIN(group_out0_0,group_out0_1,left.Group_Key=right.Group_Key,FULL OUTER);
c := join_out0_1;
d := c(COUNT(beta_records)=0);
e := TABLE(d,{Group_Key,SUM(a_records,(unsigned8)timespent),COUNT(a_records)});
OUTPUT(e,,Param_out);&#13;&#10;</Query>
</Archive>
