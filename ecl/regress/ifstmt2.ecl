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

#option ('targetClusterType', 'hthor');

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,THOR);

if (count(namesTable(age>10)) > 100, output(count(namesTable(age > 20),keyed)), output(count(namesTable(age > 30),keyed)));
if (count(namesTable(age>5)) > 100, output(count(namesTable(age > 15),keyed)), output(count(namesTable(age > 25),keyed)));
