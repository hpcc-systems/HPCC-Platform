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
string6             strsalary;
string2             nl;
                END;

testDataset := DATASET('inagg.d00', testRecord, FLAT);

a := GROUP(testDataset, forename, ALL);
b0 := SORT(testDataset, forename);
b := SORT(testDataset, forename, LOCAL);
c := SORT(testDataset, forename, JOINED(b));
d := SORT(testDataset, forename, JOINED(b), LOCAL);
e := SORT(testDataset, forename, LOCAL, JOINED(b));
f := HASH(testDataset.forename);

testRecord t(testRecord l, testRecord r) := TRANSFORM
SELF := l
    END;

g:= JOIN(testDataset, testDataset, LEFT.surname = RIGHT.surname, t(LEFT,RIGHT));
output(b,,'out.d00');
