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


set of integer8 values := [ 0, 1, 2, 3, 4, 5, 6 ] : stored('values');
set of integer8 anyValues := ALL : stored('anyValues');

integer myChoice := 3 : stored('myChoice');
integer myValue := 3 : stored('myValue');

values[myChoice];   // 2
myValue IN values;  // true
myValue IN anyValues;   // always true
myValue NOT IN anyValues;   // always false
