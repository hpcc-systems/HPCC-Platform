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


r1 := { unsigned id, string name; string name2};

r3 := { dataset(r1) children, dataset(r1) children2 };
r2 := { unsigned id, r3 r };

s := dataset([{0,[{1,'Gavin','Hawthorn'},{2,'Jason','Jones'}],[]}], r2) : persist('s');

g0 := s[1].r;
g1 := global(g0,few);
g2 := global(g1);
g := g1;


string searchString := '' : stored('search');


ds := nofold(dataset('ds', r1, thor));
ds1 := nofold(dataset('ds1', r1, thor));
ds2 := nofold(dataset('ds2', r1, thor));

t := table(ds, { name in set(g.children, name), searchString in set(g.children, name) });

output(t);

t2 := ds(name not in set(g.children, name2), name2 not in set(g.children2, name2));

//output(t2);

j := join(ds1, ds2, left.id = right.id, transform(r2, self.id := IF(left.name in set(g.children, name) or right.name in set(g.children, name), 100, 200); self := []));
//output(j);