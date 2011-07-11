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

import mod1;

f(integer m) := m + 2;

integer nx := 10;

dd := dataset('dd',{integer n;}, THOR);

dd1 := dataset('dd1',{integer n1;}, THOR);
dd2 := dataset('dd2',{integer n2;}, THOR);

mx := 30;

result := count(dd1(n1=count(dd(n=20)))) 
    + nx 
    + count(dd2(dd2.n2=mx)) + f(mx)
    + count(mod1.dd);

result;