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

//Could use a hash aggregate, but already sorted - so use grouped.
x1 := sort(testDataset, department);
x2 := table(x1,{department,count(group)},testDataset.department);
x3 := sort(x2, department);
output(x3,,'out1.d00');

y1 := sort(testDataset, forename);
y2 := table(y1,{department,count(group)},testDataset.department);
y3 := sort(y2, department);
output(y3,,'out2.d00');
