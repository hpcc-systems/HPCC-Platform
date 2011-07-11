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

//nothor
//UseStandardFiles

o1 := normalize(sqNamesTable1, left.books, transform(right));
o2 := normalize(sqNamesTable1, sort(left.books, -rating100, +author), transform(right));
o3 := normalize(sqNamesTable1, sort(left.books, +author, +name), transform({string name, string author}, self := right));

g1 := group(sqNamesTable1, surname, forename);
o4 := table(g1, { cnt := count(group), numBooks := sum(group, count(books)) });     // show there is a zero size group
o5 := normalize(g1, left.books, transform(right));                                  // grouped normalize - including an "empty" group
o6 := table(o5, { cnt := count(group) });                                           // ensure the grouping worked correctly
o7 := choosen(o5, 2);                                                               // premature finish reading.

sequential(
    output(o1), 
    output(o2), 
    output(o3),
    output(o4),
    output(o5),
    output(o6),
    output(o7),
    output('done')
);
