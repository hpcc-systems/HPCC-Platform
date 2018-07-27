/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

inputRecord :=
            RECORD
string20        surname;
string10        forename;
string3         age;
string          extra1;
string          extra2;
string          extra3;
string          extra4;
            END;

nvRecord := { string name, string value };

names := dataset([
    { 'Gavin', 'Smith', '34', 'a', 'b', 'c', 'd' },
    { 'John', 'Doe', '32', 'x', 'a', 'abc', 'q' }
    ], inputRecord);

mkpair(string x, string y) := TRANSFORM(nvRecord, SELF.name := x; SELF.value := y);

mkRecord(string x, string y) := ROW(mkpair(x, y));

mknv(unsigned c, inputRecord l) :=
    CASE(c,
     1=>mkRecord('surname', l.surname),
     2=>mkRecord('forename', l.forename),
     3=>mkRecord('age', l.age),
     4=>mkRecord('extra1', l.extra1),
     5=>mkRecord('extra2', l.extra2),
     6=>mkRecord('extra3', l.extra3),
     7=>mkRecord('extra4', l.extra4),
     mkRecord('','')
     );

n := NORMALIZE(names, 7, TRANSFORM(nvRecord, SELF := mknv(COUNTER, LEFT)));
output(n);
