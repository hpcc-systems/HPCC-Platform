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

d := dataset([{'1'}, {'2'}, {'4'}], { unsigned1 x});

d1 := d(x=1);
d2 := d(x=2);
d3 := d(x=3);
d4 := d(x=4);

o1 := output(d1);
o2 := output(d2);
o3 := output(d3);
o4 := output(d4);

string1 s := '1' : stored('s');
integer8 i := 2 : stored('i');

case(s, '1'=>o1, '2'=>o2, '3'=>o3, o4);
output(case(s, '1'=>d1, '2'=>d2, '3'=>d3, d4));

case(i, 1=>o1, 2=>o2, 3=>o3, o4);
output(case(i, 1=>d1, 2=>d2, 3=>d3, d4));

