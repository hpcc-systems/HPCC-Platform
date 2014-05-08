/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//Check that reads from grouped input, where all of a group is removed
//don't add an extra end of group.

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset([
        {'Hawthorn','Gavin',33},
        {'Hawthorn','Mia',32},
        {'Hawthorn','Abigail',0},
        {'Page','John',62},
        {'Page','Chris',26},
        {'Smithe','Abigail',13},
        {'X','Za'}], namesRecord, DISTRIBUTED);

x := group(namesTable, surname);// : persist('~GroupedNames2');

y := table(x, { countAll := count(group)});

output(y);

z := x(surname <> 'Hawthorn');


output(z, { countNonHalliday := count(group)});
