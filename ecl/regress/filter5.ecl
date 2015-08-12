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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string40 per_last_name, unsigned8 xpos }, thor);

filtered1 := person(xpos < 1000);

filter2 := filtered1(per_last_name = 'Hawthorn');
filter3 := filtered1(per_last_name = 'Drimbad');

output(filter2,,'out2.d00');
output(filter3,,'out3.d00');


namesRecord :=
            RECORD
string20        per_last_name;
string10        forename;
integer2        holepos;
            END;

namesTable := dataset('x',namesRecord,FLAT);

afiltered1 := namesTable(holepos < 1000);

afilter2 := afiltered1(per_last_name = 'Hawthorn');
afilter3 := afiltered1(per_last_name = 'Drimbad');

output(afilter2,,'aout2.d00');
output(afilter3,,'aout3.d00');

