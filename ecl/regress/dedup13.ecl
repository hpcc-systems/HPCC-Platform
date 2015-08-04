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

namesTable := dataset('x',namesRecord,FLAT);

x1 := dedup(namesTable, all);
y1 := dedup(x1, surname, all);
output(y1);

x2 := dedup(namesTable, surname, all);
y2 := dedup(x2, all);
output(y2);

x3 := dedup(namesTable, surname, forename);
y3 := dedup(x3, forename, surname);
output(y3);

x4 := dedup(namesTable, surname, forename, all);
y4 := dedup(x4, forename, surname, all);
output(y4);


g := group(namesTable, age);

xx1 := dedup(g, all);
yy1 := dedup(xx1, surname, all);
output(yy1);

xx2 := dedup(g, surname, all);
yy2 := dedup(xx2, all);
output(yy2);

xx3 := dedup(g, surname, forename);
yy3 := dedup(xx3, forename, surname);
output(yy3);

xx4 := dedup(g, surname, forename, all);
yy4 := dedup(xx4, forename, surname, all);
output(yy4);


//Now test subsets and supersets.