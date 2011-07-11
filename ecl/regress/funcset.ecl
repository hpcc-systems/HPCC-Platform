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
end;

integer testNumber := 7 : stored('testNumber');
integer testIndex := 5 : stored('testIndex');

set of integer4 values := gavLib.getPrimes();
set of integer4 valuesS := gavLib.getPrimes();// : stored('values');

values[testIndex];      // 7
values[20];             // 0
testNumber IN values;   // true

valuesS[testIndex];
valuesS[20];
testNumber IN valuesS;  // true

evaluate(100);

