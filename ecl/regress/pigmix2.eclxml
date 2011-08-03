<!--

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
