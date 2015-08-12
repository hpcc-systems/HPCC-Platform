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

#option ('optimizeProjects', true);

rec := record
unsigned f1;
unsigned f2;
unsigned f3;
end;

ds := dataset('ds', rec, thor);

rec t(rec l, rec r) := transform
    self.f3 := l.f3 + r.f3;
    self := l;
    end;

x := rollup(ds, left.f1 = right.f1 and left.f2 = right.f2, t(left, right));

output(x, {f3});
