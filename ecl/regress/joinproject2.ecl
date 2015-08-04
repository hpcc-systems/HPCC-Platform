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

names2Record :=
            RECORD
string20        surname;
string10        forename;
integer2        age2 := 25;
            END;

combRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
integer2        age2 := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);
namesTable2 := dataset('y',names2Record,FLAT);


x := sort(namesTable2, surname, -forename, local);
x2 := dedup(x, surname, local);

combRecord t1(namesRecord l, names2Record r) := transform
    self := l;
    self := r;
    end;

combRecord t2(namesRecord l, names2Record r) := transform
    self.age2 := FAILCODE;
    self := r;
    self := l;
    end;

z := join(namesTable, x2, left.surname = right.surname, t1(LEFT,RIGHT), onFail(t2(left, right)), atmost(100));

z1 := project(z, transform({left.surname, left.age2 }, self.surname := left.surname; self.age2 := left.age2 ));

output(z1);
