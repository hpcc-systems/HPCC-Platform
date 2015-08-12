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

#option ('targetClusterType', 'roxie');

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

x1 := sort(namesTable(age != 99), age);

x2 := if (count(x1)>10, x1(age<10), x1(age<20));

x3 := if (count(x1)>20, x1(age<10),x1(age<20));

x4 := if (count(x1)>30, x1(age<10),x1(age<20));

x5 := if (count(x1)>40, x1(age<10),x1(age<20));

x6 := if (count(x1)>50, x1(age<10),x1(age<20));

output (x2 +x3 + x4 + x5 + x6);
