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

gavLib := service
    set of integer4 getPrimes() : eclrtl,pure,library='eclrtl',entrypoint='rtlTestGetPrimes',oldSetFormat;
    set of integer4 getFibList(const set of integer4 inlist) : eclrtl,pure,library='eclrtl',entrypoint='rtlTestFibList',newset;
end;

output([1,2,3]+[4,5,6]);

output(ALL+[1,2]);
output([2,3]+ALL);
output([1,2,3]+All+[4,5,6]);
output(ALL+[]);

output([]+[1,2]);
output([2,3]+[]);
output([1,2,3]+[]+[4,5,6]);
output([]);

3 in ([1,2,3]+[4,5,6]);
3 in (ALL+[1,2]);
1 in [2,3]+ALL;
8 in [1,2,3]+All+[4,5,6];
