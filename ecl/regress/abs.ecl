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
integer2        age := 25;
integer2        age2 := 25;
real8           salary := 0;
string20        surname;
string10        forename;
            END;

namesTable := dataset('x',namesRecord,FLAT);

x1 := namesTable(abs(age-age2) < 20, abs(salary/age) < 1000.00);
output(x1,,'out.d00');

output(-123456D);                   // = -123456
output(ABS(-123456D));              // = 1123456
output(ABS(-1));                    // = 1
output(ABS((unsigned8)-1));         // = some very long number
output(ABS(1.0102));                    // = 1
output(ABS(-1.0102));                   // = 1
