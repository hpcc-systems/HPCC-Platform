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

ds1 := dataset('ds1', {integer m1; }, THOR);
ds2 := dataset('ds2', {integer m2; }, THOR);

r1 := record integer m1; end;
r2 := record integer m2; end;

dataset f(virtual dataset(r1) d1, virtual dataset(r2) d2) := d1(m1 = count(d2(m2=10)));

integer g(virtual dataset d) := count(d);

h(virtual dataset d1, virtual dataset d2) := g(f(d1, d2));

ct := h(ds1, ds2);

ct;
