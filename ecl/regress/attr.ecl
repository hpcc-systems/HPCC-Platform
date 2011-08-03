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
