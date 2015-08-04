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
            END;

d := dataset([{'Hawthorn','Gavin',35},
              {'Hawthorn','Abigail',2},
              {'Smith','John',57},
              {'Smith','Gavin',12}
              ], namesRecord);


namesRecordEx := record
namesRecord;
unsigned8           filepos{virtual(fileposition)};
                end;

namesTable := dataset('names',namesRecordEx,FLAT);
i := index(namesTable, { namesTable }, 'nameIndex');

output(d,,'names');
buildindex(i);

f := namesTable(keyed(age != 0));

t := table(f, { surname, forename, age, seq := random() % 100});

f2 := t(seq < 50);

t2 := table(f2, { surname, forename, unsigned4 seq := random() % age, cnt := count(i(surname = f2.surname)) });

f3 := t2(surname <> 'Hawthorn', seq != 0);

output(f3);
