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
        {'Hawthorn','Gavin',40},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',20},
        {'X','Z',10}], namesRecord);

//case 5: a row filter and a global condition
output(loop(namesTable, left.age < 40, count(rows(left)) > 1, project(rows(left), transform(namesRecord, self.age := left.age+3; self := left))));

output(loop(namesTable, left.age < 15, count(rows(left)) > 1, project(rows(left), transform(namesRecord, self.age := left.age+3; self := left))));

//The following are illegal - If they were allowedCOUNTER would need to be mapped for the filter condition.
//output(loop(namesTable, left.age < 40 - COUNTER, count(rows(left)) > 1, project(rows(left), transform(namesRecord, self.age := left.age+3; self := left))));

//output(loop(namesTable, left.age < 15 - COUNTER, count(rows(left)) > 1, project(rows(left), transform(namesRecord, self.age := left.age+3; self := left))));
