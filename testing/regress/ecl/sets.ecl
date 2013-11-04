/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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


gavLib := service
    set of integer4 getPrimes() : eclrtl,pure,library='eclrtl',entrypoint='rtlTestGetPrimes',oldSetFormat;
    set of integer4 getFibList(const set of integer4 inlist) : eclrtl,pure,library='eclrtl',entrypoint='rtlTestFibList',newset;
end;

integer2 one := 1 : stored('one');
integer2 two := 2 : stored('two');
integer2 three := 3 : stored('three');
integer2 four := 4 : stored('four');
integer2 five := 5 : stored('five');

//Different sources of sets....
set of integer4 set1 := gavLib.getPrimes();
set of integer4 set2 := [1,2,3,4,5];
set of integer4 set3 := [one,two,three,four,five];
set of integer4 set4 := [6,7,8,9,10] : stored('set4');
set of string set5 := ['one','two','three','four','five'];
set of string set6 := ['one','two','three','four','five'] : stored('set6');

ds1 := dataset(set1, { integer4 f1; });
ds2 := dataset(set2, { integer4 f1; });
ds3 := dataset(set3, { integer4 f1; });
ds4 := dataset(set4, { integer4 f1; });
ds5 := dataset(set5, { string f1; });
ds6 := dataset(set6, { string f1; });

fd1 := ds1(f1 != 99);
fd2 := ds2(f1 != 99);
fd3 := ds3(f1 != 99);
fd4 := ds4(f1 != 99);
fd5 := ds5(f1 != '');
fd6 := ds6(f1 != '');

x1 := set(fd1, f1);
x1b := set(fd1, f1*2);
x2 := set(fd2, f1);
x3 := set(fd3, f1);
x4 := set(fd4, f1);
x5 := set(fd5, f1);
x6 := set(fd6, f1);

output(fd1);
output(fd2);
output(fd3);
output(fd4);
output(fd5);
output(fd6);

integer4 search1 := 2 : stored('search1');
integer4 search2 := 3 : stored('search2');
integer4 search3 := 10 : stored('search3');
string searchText := '' : stored('searchText');
output(search1 IN x1);      // true
output(search1 IN x1b);     // true 1*2
output(search2 IN x1);      // true
output(search2 IN x1b);     // false
output(search1 IN x2);      // true
output(search1 IN x3);      // true
output(search1 IN x4);      // false
output(search3 IN x4);      // true
output(x1);
output(x1b);
output(x2);
output(x3);
output(x4);
output(x5);
output(x6);

output(dataset(set1, {integer4 f1;}));  // 1, 2, 3, 5, 7, 11
fib1 := gavLib.getFibList(set1);
output(dataset(fib1, {integer4 f1;}));  // 1, 3, 5, 8, 12, 18

set1 = ALL;
set1 != ALL;
set1 = [];
set1 != [];
set2 = ALL;
set2 != ALL;
set2 = [];
set2 != [];
