/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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


person_layout := RECORD
    unsigned8 person_id,
    string1 per_sex,
    string40 per_last_name,
    unsigned8 xpos
END;
person := dataset('person', person_layout, thor);

isLessXpos1000 := LIKELY(person.xpos < 1000, 0.1);
isIdLess4 := LIKELY(person.person_id < 4,0.2);
filtered0 := person( LIKELY(isLessXpos1000, 0.2) );
filtered1 := person( isLessXpos1000 AND isIdLess4 );
filtered2 := person( isLessXpos1000 OR isIdLess4 );
filtered3 := person( NOT isLessXpos1000 );
filtered4 := person( LIKELY(TRUE) );

output(filtered0,,'tst_likely2.d00');
output(filtered1,,'tst_likely2.d01');
output(filtered2,,'tst_likely2.d02');
output(filtered3,,'tst_likely2.d03');
output(filtered4,,'tst_likely2.d04');

