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

salary      := (integer)testDataset.strsalary;


newDataset := table(testDataset,{'<',(varstring2)123,count(group),surname,(string10)sum(group, salary),
  (string10)ave(group, salary),max(group,surname)+min(group,surname),max(group,surname),'>'},
  testDataset.surname);

output(newDataset,,'out.d00');
