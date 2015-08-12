/*##############################################################################

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

j := join(namesTable, namesTable, left.age=right.age and Sec_Range_EQ(left.surname, right.surname) < 5);
output(j);
