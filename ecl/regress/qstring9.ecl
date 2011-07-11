/*##############################################################################

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
############################################################################## */



hsr(string sr) := stringlib.stringfilter(sr,'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789');
num_only(string sr) := stringlib.stringfilter(sr,'0123456789');
let_only(string sr) := stringlib.stringfilter(sr,'ABCDEFGHIJKLMNOPQRSTUVWXYZ');

desig(string20 sr) := MAP( stringlib.stringfind(trim(sr),' ',1)<>0 => sr[stringlib.stringfind(trim(sr),' ',1)+1..],
                         stringlib.stringfind(trim(sr),'-',1)<>0 => sr[stringlib.stringfind(trim(sr),'-',1)+1..],
                         sr[1] >= 'A' and sr[1]<='Z' and (unsigned4)(sr[2..length(sr)])<>0=> sr[2..],
                         sr[length(trim(sr))] >= 'A' and sr[length(trim(sr))]<='Z' and (unsigned4)(sr[1..length(trim(sr))-1])<>0=> sr[1..length(trim(sr))-1],
                         sr );

export Sec_Range_EQ(string le, string ri) := 
  MAP( le=ri => 0,
       le = '' or ri = '' or hsr(le)=hsr(ri) => 1,
       desig(le)=desig(ri) => 2, 
       num_only(le)=num_only(ri) and (unsigned4)num_only(le)<>0 => 3,
       let_only(le)=let_only(ri) and (unsigned4)num_only(le)=0 and let_only(le)<>'' => 4,
       10 );


namesRecord := 
            RECORD
qstring20       surname;
qstring10       forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);


scoreFunc(string x) := map(x = ''=>0,
                           x between 'a' and 'z'=>1,
                           2);

j := join(namesTable, namesTable, left.age=right.age and (string)(left.surname[1]) = (string)(right.surname[1]));
output(j);
