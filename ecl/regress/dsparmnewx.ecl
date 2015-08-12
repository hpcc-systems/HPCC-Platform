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

import mod1;

f(integer m) := m + 2;

integer nx := 10;

dd := dataset('dd',{integer n;}, THOR);

dd1 := dataset('dd1',{integer n1;}, THOR);
dd2 := dataset('dd2',{integer n2;}, THOR);

mx := 30;

result := count(dd1(n1=count(dd(n=20))))
    + nx
    + count(dd2(dd2.n2=mx)) + f(mx)
    + count(mod1.dd);

result;