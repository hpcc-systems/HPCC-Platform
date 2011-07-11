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

sillyrec := record
string25 name;
string200 contents;
end;

ds := dataset('~thor::updatetest::datafile', sillyrec, thor);

ds1 := dataset([{'Megan', 'Baltimore'}], sillyrec);
ds2 := sort(ds, name) + sort(ds1, name);
output(ds2,, '~thor::updatetest::datafile', overwrite);
// Results in crc error reading the file, and the contents are then
// two copies of the "Megan", "Baltimore" data
/*
 // Code to create the original data set
ds := dataset([{'Bruce', 'Germantown QW'}], sillyrec);

output(ds,,'updatetest::datafile', overwrite);
*/