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
unsigned8       filepos{virtual(fileposition)};
            END;

d := dataset('x',namesRecord,FLAT);


i1 := index(d, { d } ,'\\home\\person.name_first.key1');
i2 := index(d, { d } ,'\\home\\person.name_first.key2');

boolean whichIndex := false : stored('which');

x1 := i1(keyed(surname='Hawthorn'));
x2 := i2(keyed(surname='Hawthorn'));

i := if(whichIndex, x1, x2);
a2 := limit(i, 2000);
output(a2);
