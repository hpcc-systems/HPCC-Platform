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

namesTable := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'Smithe','Pru',10},
        {'Smithe','Pru',10},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

x := table(namesTable, {unsigned4 c1 := count(group,age=10), unsigned4 c2 := count(group,age>30), unsigned4 c3 := count(group,age>20), string m1 := max(group, forename); });
y := x[1] : stored('counts');

y.c1;
y.c2;
y.c3;

