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

r1 := { unsigned i };
r2 := { unsigned i, dataset(r1) ii; };
r3 := { unsigned i, dataset(r2) jj; };

ds1a := dataset([3,4],r1);
ds1b := dataset([6,7],r1);
ds1c := dataset([10,11],r1);
ds2a := dataset([{2,ds1a},{5,ds1b}],r2);
ds2b := dataset([{9,ds1c}],r2);
ds3 := dataset([{1,ds2a},{8,ds2b}],r3);

gr := GROUP(nofold(ds2a),  i) : global;


f(unsigned x) := sort(project(gr, transform(recordof(gr), self.i := left.i + x; self := left)), i);
d2 := dataset([1,2], { unsigned val });
p := project(d2, transform({ unsigned cnt, dataset(recordof(gr)) cd; }, SELF.cnt := count(table(f(left.val), { count(group) })); self.cd := ungroup(f(left.val))));
output(p);
