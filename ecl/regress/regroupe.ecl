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


inrec := record
unsigned6 did;
    end;

outrec := record(inrec)
string20        name;
string10        ssn;
unsigned8       dob;
          end;

ds := dataset('ds', inrec, thor);



i1 := index(ds, { unsigned6 did, string10 name }, { } ,'\\key1');
i2 := index(ds, { unsigned6 did, string10 ssn }, { } ,'\\key2');
i3 := index(ds, { unsigned6 did, unsigned8 dob }, { } ,'\\key3');


j1 := join(ds, i1, left.did = right.did, left outer);
j2 := join(ds, i2, left.did = right.did, left outer);
j3 := join(ds, i3, left.did = right.did, left outer);

combined1 := combine(j1, j2, transform(outRec, self := left; self := right; self := []));
combined2 := combine(combined1, j3, transform(outRec, self := left; self := right));
output(combined2);
