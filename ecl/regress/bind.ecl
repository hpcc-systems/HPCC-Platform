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


/*
a(integer x, integer y) := x * y;
b(integer x, integer y) := a(x, y) + a(x, y);
c(integer x, integer y) := b(x, y) + b(x, y);

d(integer x, integer y, integer z) := c(1, 2) + c(1, 2) + z;
e(integer x, integer y) := d(x, y, 8) + d(x, y, 9);
*/

f(integer x, integer y) := (x * y);
g(integer x, integer y, integer z) := f(x, y) + f(x, y) + 1;
h(integer x, integer y) := g(x, y, 8) + g(x, y, 9);

//output(person,{c(1,3)},'out.d00');    // Need to common up expansions of expansions.
//output(person,{e(1,3)},'out.d00');    // Don't expand again if already fully bound
person := dataset('person', { unsigned8 person_id, string1 per_sex; }, thor);
output(person,{h(1,3)},'out.d00');      // need to common up if parameters are insignificant
