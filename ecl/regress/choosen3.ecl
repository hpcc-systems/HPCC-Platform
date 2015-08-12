/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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

