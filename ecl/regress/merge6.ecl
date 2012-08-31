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

#option ('targetClusterType', 'roxie');

myfile := dataset('c', {string10 d, unsigned8 fpos { virtual(fileposition)}}, THOR);

f1 := SORTED(INDEX(myfile, { d, fpos }, 'f1'), d, fpos);
f2 := SORTED(INDEX(myfile, { d, fpos }, 'f2'), d, fpos);

m := merge(f1, f2, sorted(d));;
output(m);
output(f1);


//Check sort order is tracked correctly by seing if inputs are resorted
j1 := join(m, m, left.d = right.d);
output(j1);

j2 := join(m, m, left.d = right.d and left.fpos = right.fpos);
output(j2);