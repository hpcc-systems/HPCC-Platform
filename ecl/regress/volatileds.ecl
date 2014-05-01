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

import Std.System.Debug;

//You cannot extract the nth element from an inline dataset, otherwise comparing the values will give you different results.
ds1 := dataset([1, 2, 3, random(), random(), 6], { unsigned id; });

output(ds1[3].id);
output(ds1);


// You cannot duplicate a row that is volatile

row1 := DATASET(ROW(TRANSFORM({unsigned id}, SELF.id := RANDOM())));
output(normalize(row1, 10, transform(LEFT)));

// Merging an input project with a join could cause issues

ds2 := DATASET('x', { unsigned id1 }, THOR);
p2 := PROJECT(ds2, TRANSFORM({ unsigned id1, unsigned id2 }, SELF.id2 := RANDOM(), SELF := LEFT));


ds3 := DATASET('y', { unsigned id1, unsigned id2 }, THOR);

output(JOIN(p2, ds3, LEFT.id1 = RIGHT.id1 AND LEFT.id2 = RIGHT.id2, LOOKUP));

//Resourcing must never clone a volatile activity
ds4 := dataset([1, random(), random(), 6], { unsigned id; });
output(sort(ds4, id));
output(sort(ds4, -id));
