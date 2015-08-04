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

testRecord := RECORD
string10            forename;
string10            surname;
string10            department{CARDINALITY(1000)};
string6             strsalary;
string2             nl;
                END;

testDataset := DATASET('inagg.d00', testRecord, FLAT);


x1 := table(testDataset,{surname,count(group)},testDataset.surname,sorted);
x2 := sort(x1, surname);
x3 := table(testDataset,{surname,count(group)},testDataset.surname, FEW);
x4 := sort(x3, surname);
x5 := table(testDataset,{department,count(group)},testDataset.department);

output(x1,,'out1.d00');
output(x2,,'out2.d00');
output(x3,,'out3.d00');
output(x4,,'out4.d00');
output(x5,,'out5.d00');
