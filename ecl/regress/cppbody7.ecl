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



integer4 mkRandom1 :=
BEGINC++
rtlRandom()
ENDC++;

integer4 mkRandom2 :=
BEGINC++
#option pure
rtlRandom()
ENDC++;

integer4 mkRandom3 :=
BEGINC++
#option action
rtlRandom()
ENDC++;

output(mkRandom1 * mkRandom1);
output(mkRandom2 * mkRandom2);
output(mkRandom3 * mkRandom3);
