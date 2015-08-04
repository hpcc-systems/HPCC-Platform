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

namesTable := distributed(dataset('x',namesRecord,FLAT),hash(forename));


qnamesRecord :=
            RECORD
qstring20       surname;
qstring10       forename;
integer2        age := 25;
            END;

qnamesTable := distributed(dataset('x',qnamesRecord,FLAT),hash(forename));


sortNames := sort(namesTable, surname,local);
qsortNames := sort(qnamesTable, surname,local);

output(join(sortNames(age=1),sortNames(age=2),LEFT.surname=RIGHT.surname,local));
output(join(qsortNames(age=1),qsortNames(age=2),LEFT.surname=RIGHT.surname,local));
output(join(qsortNames(age=1),sortNames(age=2),LEFT.surname=RIGHT.surname,local));
output(join(sortNames(age=1),qsortNames(age=2),LEFT.surname=RIGHT.surname,local));

sortNamesQStr := sort(namesTable, (qstring)surname,local);
qsortNamesStr := sort(qnamesTable, (string)surname,local);

//Some of these should match as sorted, but very uncommon, and doesn't fall out easily
output(join(sortNamesQStr(age=1),sortNamesQStr(age=2),LEFT.surname=RIGHT.surname,local));
output(join(qsortNamesStr(age=1),qsortNamesStr(age=2),LEFT.surname=RIGHT.surname,local));
output(join(qsortNamesStr(age=1),sortNamesQStr(age=2),LEFT.surname=RIGHT.surname,local));
output(join(sortNamesQStr(age=1),qsortNamesStr(age=2),LEFT.surname=RIGHT.surname,local));
