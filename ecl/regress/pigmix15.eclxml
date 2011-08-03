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
