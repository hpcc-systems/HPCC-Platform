/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#option ('generateLogicalGraph', true);

// A single dataset that gets split into two and then joined back together 
// The transform contains a count from the same dataset - how is this rendered?

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

s := SORT(namesTable(surname != ''), forename);

delta := TABLE(s(age != 0), { unsigned cnt := COUNT(GROUP) })[1].cnt;

namesRecord t(namesRecord l) := TRANSFORM
    SELF.age := l.age + delta;
    SELF := l;
END;

p:= project(s, t(LEFT));

f1 := p(age < 18);
f2 := p(age >= 18);

output(sort(f1 + f2, surname));

