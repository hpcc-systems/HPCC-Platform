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

// Horrible test for success/failure workflow requring sequential/conditional children.

s1 := false : stored('s1');

o1 := output(100) : persist('o1');
o1b := output(99) : persist('o1b');

o2 := sequential(o1, o1b) : independent;

o3 := output(101) : persist('o3');

of := output(-1);

o4 := if (s1, o2, o3) : failure(of);



of1 := output(-2) : persist('of1');
of2 := output(-3) : persist('of2');

ofs := sequential(of1, of2);

o5 := o4 : failure(ofs);


o5;
