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
