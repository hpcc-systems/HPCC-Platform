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
unsigned8       filepos{virtual(fileposition)}
            END;

namesTable := dataset('x',namesRecord,FLAT);


a := LIMIT(namesTable, 1000);

b := a(surname <> 'Hawthorn');

c := LIMIT(b, 800, FAIL(99, 'Oh dear too many people'));

output(c,,'out.d00');

d := CASE(namesTable.age, 10=>'Ok',11=>'Poor',99=>ERROR('99 Not allowed'), 'Pass');

e := CASE(namesTable.surname, 'Hawthorn'=>1,'Drimbad'=>2,ERROR('Not a know surname'));

f := IF(namesTable.forename = 'Gavin', 'Ok', ERROR('Just not good enough'));

output(namesTable, {surname, forename, age, d, e, f});



i := index(namesTable, { surname, forename, filepos } ,'\\home\\person.name_first.key');

string10 searchName := 'Hawthorn'       : stored('SearchName');

x := limit(i(surname=searchName), 100);

output(x);


j := JOIN(namesTable, i, LEFT.surname=RIGHT.surname);

k := LIMIT(j, 99);

output(k);


l := limit(a(surname <> 'Drimbad'), 800, FAIL(99, 'Oh dear too many people'));

output(l);

m := limit(a(forename <> 'Richard'),999);

output(m);
