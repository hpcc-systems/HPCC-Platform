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

//Test combining funnels

householdRecord := RECORD
unsigned4 house_id;
string20  address1;
string20  zip;
string2   st;
    END;


householdDataset := DATASET('household',householdRecord,FLAT);

x := householdDataset + householdDataset(st = 'NY') + householdDataset + householdDataset(st = 'FL');

output(x,,'out1.d00');

x2 := (householdDataset & householdDataset(st = 'NY')) + (householdDataset & householdDataset(st = 'FL'));
output(x2,,'out2.d00');

x3 := (householdDataset & householdDataset(st = 'NY')) & (householdDataset & householdDataset(st = 'FL'));
output(x3,,'out3.d00');

x4 := householdDataset & householdDataset(st = 'NY') & householdDataset + householdDataset(st = 'FL');
output(x4,,'out4.d00');

x5 := (householdDataset && householdDataset(st = 'NY')) && (householdDataset && householdDataset(st = 'FL'));
output(x5,,'out5.d00');
