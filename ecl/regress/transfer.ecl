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

#option ('foldAssign', false);
#option ('globalFold', false);
testRecord := RECORD
string10            forename;
integer4            surname;
string2             strsalary;
string2             nl;
                END;

testDataset := DATASET([{'a','a','a','a'}], testRecord);

newDataset := table(testDataset, {
    transfer('gavin',string4),
    (string3)transfer('A',unsigned1),
    transfer(99%256,string1)});

/*
newDataset := table(testDataset, {
    transfer('01',integer2),
    transfer(forename,integer4),
    transfer(forename[1..4],integer4),
    transfer(surname,string4),
    transfer(surname,integer2),
    transfer('gavin',string4)});
*/

output(newDataset,,'out.d00');
