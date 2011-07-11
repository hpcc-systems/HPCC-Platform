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

#option ('targetClusterType', 'roxie');

myfile := dataset('c', {string10 d, unsigned8 fpos { virtual(fileposition)}}, THOR);

f1 := SORTED(INDEX(myfile, { d, fpos }, 'f1'), d, fpos);
f2 := SORTED(INDEX(myfile, { d, fpos }, 'f2'), d, fpos);

m := merge(f1, f2, sorted(d));;
output(m);
output(f1);


//Check sort order is tracked correctly by seing if inputs are resorted
j1 := join(m, m, left.d = right.d);
output(j1);

j2 := join(m, m, left.d = right.d and left.fpos = right.fpos);
output(j2);