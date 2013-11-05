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


namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

twenty    := 20 : stored('twenty');
twentyone := 21 : stored('twentyone');
twentytwo := 22 : stored('twentytwo');


d1 := dataset([{'Jones','Jim',twenty}], namesRecord);
d2 := dataset([{'Jones','John',twentyone}], namesRecord);
d3 := dataset([{'Jones','James',twentytwo}], namesRecord);
d4 := dataset([{'Jones','Jimmy',twenty}], namesRecord);
d5 := dataset([{'Jones','Jonnie',twentyone}], namesRecord);

output(nonempty(d1(age != 40), d2(age != 40)));
output(nonempty(d1(age != 20), d2(age != 40)));
output(nonempty(d1(age != 40), d2(age != 21)));
output(nonempty(d1(age != 20), d2(age != 21)));
output(nonempty(d1, d2, d3, d4));
output(nonempty(d1(age = 0), d2(age = 0), d3(age = 0), d4));
output(nonempty(d1(age = 0), d2(age = 0), d3, d4));

