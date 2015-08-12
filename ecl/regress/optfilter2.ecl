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

// optfilter2
// filter should be moved before the project and then combined with other filter
// Also a good example for read/filter/project optimization

testRecord := RECORD
unsigned4 person_id;
string20  per_surname;
string20  per_forename;
unsigned8 holepos;
    END;

test := DATASET('test',testRecord,FLAT);

a := test(per_surname <> 'Hawthorn');

b := table(a,{per_surname,per_forename});

c:= b(per_forename <> 'Gavin');

output(c,,'out.d00');
