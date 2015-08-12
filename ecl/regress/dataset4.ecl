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

#option ('optimizeGraph', false);
//Check that spilling continues to read all the records, even if input is terminated.
namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Hawthorn','Abigail',0},
        {'Page','John',62},
        {'Page','Chris',26},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

w := sort(namesTable, surname);
x := namesTable(age != 100);

y1 := choosen(x, 1);
y2 := sort(x, forename)(surname <> 'Page');

output(y1);
output(y2);
output(y1);


