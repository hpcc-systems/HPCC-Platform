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



integer4 add1(integer4 x, integer4 y) :=
BEGINC++
return x + y;
ENDC++;

integer4 add2(integer4 x, integer4 y) :=
BEGINC++
#option action
return x + y;
ENDC++;

integer4 add3(integer4 x, integer4 y) :=
BEGINC++
#option pure
return x + y;
ENDC++;

output(add1(10,20) * add1(10,20));
output(add2(10,20) * add2(10,20));
output(add3(10,20) * add3(10,20));
