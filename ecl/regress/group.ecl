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
string20        surname;
string10        forename;
integer2        age := 25;
string10        forename1 := '';
string10        forename2 := '';
string10        forename3 := '';
string10        forename4 := '';
string10        forename5 := '';
            END;

namesTable := dataset([
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'Hawthorn','Gavin',31},
        {'X','Z'}], namesRecord);

//output(sort(namesTable,surname),{surname},'out.d00');

//output(group(namesTable,surname ALL),{surname,count(group)},'out.d00');
output(group(sort(namesTable,surname),surname),{surname,count(group)},'out0.d00');
output(table(namesTable,{surname,count(group)},surname),,'out1.d00');

a1 := sort(namesTable, namesTable.forename1, namesTable.forename2, namesTable.forename3, - namesTable.forename4, namesTable.forename5, LOCAL);
a2 := GROUP(a1, a1.forename1, a1.forename2, a1.forename3, LOCAL);

namesRecord t(namesRecord l) :=
    TRANSFORM
        SELF := l;
    END;

a3 := ITERATE(a2, t(LEFT));

output(a3);
