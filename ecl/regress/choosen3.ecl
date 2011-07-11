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

i := dataset([{1}, {3}, {5}, {7}, {9}, {2}, {4}, {6}, {8}, {10}], {unsigned1 d});
s := sort(i, d);
output(choosen(s, 100, 5));

i2 := dataset('i2', {unsigned1 d}, thor);
output(choosen(i2, 100, 5));

i3 := dataset('i2', {unsigned1 d}, xml);
s3 := sort(i3, d);
output(choosen(s3, 100, 5));

unsigned4 firstReturned := 1 : stored('first');
unsigned4 num := 100 : stored('num');

i4 := dataset('i4', {unsigned1 d}, thor);
output(choosen(i4, num, firstReturned));

i5 := dataset('i5', {unsigned1 d}, xml);
s5 := sort(i5, d);
output(choosen(s5, 100, 1));

i6 := dataset('i6', {unsigned1 d}, thor);
output(choosen(i6, 1, 100));

i7 := dataset('i7', {unsigned1 d}, thor);
s7 := sort(i7, d);
output(choosen(s7, 1, 2));

i8 := dataset('i8', {unsigned1 d}, thor);
s8 := sort(i8, d);
output(choosen(s8, -2, -3));

integer4 firstReturned2 := 1 : stored('first2');
integer4 num2 := 100 : stored('num2');

i9 := dataset('i9', {unsigned1 d}, thor);
output(choosen(i9, num2, firstReturned2));

