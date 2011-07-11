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

#option ('globalFold', false);
unicode x := U'abc def  ' : stored('x');
unicode y := U'def ghi' : stored('y');

integer one := 1 : stored('one');
integer four := 4 : stored('four');
integer five := 5 : stored('five');
integer six := 6 : stored('six');
integer seven := 7 : stored('seven');
integer ten := 10 : stored('ten');

'The following should be true:';
trim(x[four]) = trim(y[four]);
trim(y[1..four]) = trim(x[five..]);
trim(y[1..four]) = trim(x[five..ten]);
trim(y[1..four]) = trim(x[five..seven]);
trim(x[five..four]) = trim(y[ten..]);

'The following should be false:';
trim(x[five..seven]) = trim(y[1..five]);
trim(x[five..seven]) = trim(y[one..five]);
