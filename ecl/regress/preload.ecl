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

namesTable := dataset('x',namesRecord,FLAT,preload);

x1 := namesTable(surname[1] in ['0','1','3']);
x2 := table(x1, {forename, surname});
output(x2);

namesTable2 := dataset('x',namesRecord,FLAT);

y1 := namesTable2(surname[1] in ['0','1','3']);
y2 := preload(y1);
y3 := table(y2, {forename, surname});
output(y3);


d := dataset('~local::rkc::person', { string15 name, unsigned8 filepos{virtual(fileposition)} }, flat);
i1 := index(d, { f_name := (string11) name, filepos } ,'\\home\\person.name_first.key',preload);
output(i1(f_name='Gavin'));

i2 := index(d, { f_name := (string11) name, filepos } ,'\\home\\person.name_first.key');
output(preload(i2)(f_name='Hawthorn'));

namesTable3 := dataset('x',namesRecord,FLAT,preload(1+2+3+4));
output(namesTable3(age=3));
