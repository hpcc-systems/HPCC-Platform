/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
