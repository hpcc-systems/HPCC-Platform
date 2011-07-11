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

ds := dataset([1,2,3,4,5,6], inrec);

i1 := dataset([{1, 'Gavin'}, {2, 'Richard'}, {5,'Nigel'}], { unsigned6 did, string10 name });
i2 := dataset([{3, '123462'}, {5, '1287234'}, {6,'007001002'}], { unsigned6 did, string10 ssn });
i3 := dataset([{1, 19700117}, {4, 19831212}, {6,20000101}], { unsigned6 did, unsigned8 dob});

j1 := join(ds, i1, left.did = right.did, left outer, lookup);
j2 := join(ds, i2, left.did = right.did, left outer, lookup);
j3 := join(ds, i3, left.did = right.did, left outer, lookup);

combined1 := combine(j1, j2, transform(outRec, self := left; self := right; self := []));
combined2 := combine(combined1, j3, transform(outRec, self.dob := right.dob; self := left));
output(combined2);

