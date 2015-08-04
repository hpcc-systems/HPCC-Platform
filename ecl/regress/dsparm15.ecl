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

ds := dataset('ds', {integer m1; integer m2; }, THOR);

r1 := record integer n1; end;
r2 := record integer n2; integer n3 end;

dataset f(virtual dataset(r1) d) := d(n1 = 10);

dataset g(virtual dataset(r2) d) := d(n3 = max(f(d{n1:=n2}),n3));

ct := count(g(ds{n2:=m1; n3:=m2;}));

ct;
