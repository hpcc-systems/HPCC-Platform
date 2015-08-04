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

test2Record := RECORD
string10            newforename;
string10            newsurname;
string6             newstrsalary;
string2             newnl;
                END;

testDataset := DATASET('inagg.d00', testRecord, FLAT);


x1 := table(testDataset,{surname,count(group)},testDataset.surname);
x2 := sort(x1, surname);

x3 := sort(testDataset, surname);
x4 := table(x3,{surname,count(group)},surname);

test2Record t(testRecord l) := TRANSFORM
        SELF.newforename := l.forename;
        SELF.newsurname := l.surname;
        SELF.newstrsalary := l.strsalary;
        SELF.newnl := l.nl;
    END;

x5 := project(x3, t(LEFT));
x6 := table(x5,{newsurname,count(group)},newsurname);

output(x2,,'out3.d00');
output(x4,,'out4.d00');
output(x6,,'out5.d00');
