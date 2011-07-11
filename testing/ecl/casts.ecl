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

t := true : stored('t'); // true, but value is unknown at compile time
f := ((integer)t) < 0.5; // should be false, but is true
output(t,named('trueval'));
output(f,named('falseval'));


t2 := true ; // true, but value is unknown at compile time
f2 := ((integer)t2) < 0.5; // should be false, but is true
output(t2,named('trueval2'));
output(f2,named('falseval2'));


integer4 minus1 := -1 : stored('minus1');
integer4 minus2 := -2 : stored('minus2');

output((real)minus1 < -1.5,named('false_3'));
output((real)minus2 < -1.5,named('true_3'));
output(-1.5 > (real)minus1 ,named('false_4'));
output(-1.5 > (real)minus2,named('true_4'));

output((real)minus1 <= -1.5,named('false_5'));
output((real)minus2 <= -1.5,named('true_5'));
output(-1.5 >= (real)minus1 ,named('false_6'));
output(-1.5 >= (real)minus2,named('true_6'));

integer4 plus1 := +1 : stored('plus1');
integer4 plus2 := +2 : stored('plus2');

output((real)plus1 < +1.5,named('true_7'));
output((real)plus2 < +1.5,named('false_7'));
output(+1.5 > (real)plus1 ,named('true_8'));
output(+1.5 > (real)plus2,named('false_8'));

output((real)plus1 <= +1.5,named('true_9'));
output((real)plus2 <= +1.5,named('false_9'));
output(+1.5 >= (real)plus1 ,named('true_10'));
output(+1.5 >= (real)plus2,named('false_10'));


integer1 small := 1 : stored('small');

output((real)small < -1000.5,named('falsex_1'));
output(-1000.5 > (real)small ,named('falsex_2'));
output((real)small <= -1000.5,named('falsex_3'));
output(-1000.5 >= (real)small ,named('falsex_4'));

output((real)small < +1000.5,named('truex_1'));
output(+1000.5 > (real)small ,named('truex_2'));
output((real)small <= +1000.5,named('truex_3'));
output(+1000.5 >= (real)small ,named('truex_4'));

output((real)small != -1000.5,named('truex_5'));
output(-1000.5 = (real)small ,named('falsex_5'));
output((real)small != +1000.5,named('truex_6'));
output(+1000.5 = (real)small ,named('falsex_6'));
