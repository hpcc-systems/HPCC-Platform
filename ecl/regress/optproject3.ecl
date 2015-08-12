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

// optproject3
// Move projections as early as possible., but include sort fields

testRecord := RECORD
unsigned4 person_id;
string20  per_surname;
string20  per_forename;
unsigned8 holepos;
    END;

test := DATASET('test',testRecord,FLAT);

a := sort(test,person_id);

output(a,{per_surname},'out.d00');
