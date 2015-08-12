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


namesRecord :=
            RECORD
string      surname;
qstring     forename;
integer2        age := 25;
                ifblock(self.age != 99)
unicode     addr := U'12345';
varstring   zz := self.forename[1] + '. ' + self.surname;
varunicode  zz2 := U'!' + (unicode)self.age + U'!';
boolean         alive := true;
data5           extra := x'12345678ef';
                end;
            END;

namesTable := dataset('x',namesRecord,FLAT);

namesTable2 := dataset([
        {'Time','Old Father',1000000},
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'Hawthorn','Abigail',0},
        {'South','Ami',2},
        {'X','Z'}], namesRecord);

output(namesTable2,,'out.d00',overwrite);
output(namesTable,{1,2,3,4});
