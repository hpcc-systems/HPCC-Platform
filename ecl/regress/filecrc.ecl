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

#option ('targetClusterType', 'hthor');

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesRecordEx :=
            RECORD(namesRecord)
unsigned8       filepos{virtual(fileposition)}
            END;


namesRecordEx2 :=
            RECORD
namesRecord;
unsigned8       filepos{virtual(fileposition)}
            END;

namesTableRaw := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

namesTable := dataset('crcNamesTable',namesRecord,FLAT);
namesTableEx := dataset('crcNamesTable',namesRecordEx,FLAT);
namesTableEx2 := dataset('crcNamesTable',namesRecordEx2,FLAT);

namesIndexEx := index(namesTableEx, { namesTableEx }, 'crcNamesIndexEx');
namesIndexEx2 := index(namesTableEx2, { namesTableEx2 }, 'crcNamesIndexEx2');

ds := dataset([0, 64, 32], { unsigned fpos });

output(namesTableRaw,,'crcNamesTable');
buildindex(namesIndexEx);
buildindex(namesIndexEx2);

output(namesTable(age != 0));
output(namesTableEx(age != 0));
output(namesTableEx2(age != 0));


output(fetch(namesTable, ds, right.fpos, transform(left)));
output(fetch(namesTableEx, ds, right.fpos, transform(left)));
output(fetch(namesTableEx2, ds, right.fpos, transform(left)));

output(join(namesTable, namesIndexEx, left.surname = right.surname));
output(join(namesTable, namesIndexEx2, left.surname = right.surname));
output(join(namesTable, namesTableEx, left.surname = right.surname, keyed(namesIndexEx)));
output(join(namesTable, namesTableEx2, left.surname = right.surname, keyed(namesIndexEx2)));
