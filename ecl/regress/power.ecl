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

person := dataset('person', { unsigned8 person_id, string1 per_sex, unsigned per_ssn, string40 per_first_name, data9 per_cid, unsigned8 xpos }, thor);

x:= power(person.person_id, 2);
y := power(10,1);
z := power(10,2);

output(person,
    {
    power(10,1),
    power(10,1),
    power(10,1),
    power(10,1),
    x1:= x,
    x2:= x,
    x3 := x,
//  y1 := y,
//  y2 := y,
//  y3 := y,
    z1 := z;
    if (x != 0, 1/x, x*2),
    if (y != 0, 1/y, y*2),
    0
    },'out.d00');