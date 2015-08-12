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
#option ('globalFold', false);
// optfilter
// filter should be moved before the project.  Also a good example for
// read/filter/project optimization

testRecord := RECORD
string20  per_surname;
string20  per_forename;
unsigned8 holepos;
integer2  age;
    END;

test := DATASET('test',testRecord,FLAT);

a := table(test,{per_surname,per_forename, test.age});

b:= a(per_surname <> 'Hawthorn' and a.per_forename <> 'Gavin');

output(b,,'out.d00');

c := table(test,{per_surname,per_forename, test.holepos});

d := table(c,{per_surname,c.per_forename, holepos});

e := d(d.holepos > 10 and per_surname <> '');

output(e,,'oute.d00');
