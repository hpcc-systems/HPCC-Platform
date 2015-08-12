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

#option ('targetClusterType', 'roxie');

string skipName := '' : stored('skipName');
unsigned skipNumber := 0 : stored('skipNumber');

r0 := { unsigned z };
r1 := { dataset(r0) c0, string name, string d };
r2 := { dataset(r1) cr, unsigned id, string x; };

i := index({ unsigned id }, { string x }, 'i');

ds := dataset('ds', { string abc; integer value; dataset(r2) c2 }, thor);


removeSkips(dataset(r1) inFile) := function

    r1 t(r1 l) := transform
        self.c0 := sort(l.c0(z != skipNumber), z);
        self := l;
    end;

    return project(inFile, t(left));
end;

nonLocalProcess(dataset(r2) inFile, string skipper) := function

    px := inFile((string)id != skipper);
    kj := join(inFile, i, left.id = right.id, transform(r2, self.x := right.x, self := left));


    r2 t2(r2 l) := transform
        self.cr := sort(removeSkips(l.cr)(name != skipName), name);
        self := l;
        end;

    p2 := project(nofold(kj), t2(left));

    return p2;
end;



ds t1(ds l) := transform
    self.c2 := nonLocalProcess(l.c2, l.abc);
    self := l;
    end;

p1 := project(ds, t1(left));

output(p1);


