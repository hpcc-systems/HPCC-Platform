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

r := { string15 name := '', unsigned8 filepos{virtual(fileposition)} := 0};

d := dataset('~local::rkc::person', r, flat);

i := index(d, r,'\\home\\person.name_first.key');

a1 := i(name='RICHARD');

a2 := sort(a1, -name);

output(a2(filepos > 10));
output(a2(filepos > 20));

buildindex(i);