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

namerec := {UNSIGNED1 i, STRING5 str};

names1 := nofold(DATASET([{1, 'one'}, {4, 'four'}, {7, 'seven'}, {8, 'eight'}, {9, 'nine'}], namerec));
output(names1, named('names'),thor);

output(dataset(workunit('names'), namerec), NAMED('namesClone'), thor);

nameClone := dataset(workunit('namesClone'), namerec);
next(unsigned1 search) := nofold(sort(nofold(nameClone(i > search)), i))[1];

names2 := nofold(DATASET([{2, 'two'}, {3, 'three'}, {4, 'four'}, {9, 'nine'}], namerec));
p := project(names2, transform(namerec, self.i := left.i; self.str := next(left.i).str));
output(p);
