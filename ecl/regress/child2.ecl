/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */


r1 := { unsigned id, string name; string name2};

r2 := { unsigned id, dataset(r1) children, dataset(r1) children2 };

s := dataset([{0,[{1,'Gavin','Halliday'},{2,'Jason','Jones'}],[]}], r2) : persist('s');

g0 := s[1];
g1 := global(g0,few);
g2 := global(g1);
g := g1;


string searchString := '' : stored('search');


ds := nofold(dataset('ds', r1, thor));

t := table(ds, { name in set(g.children, name), searchString in set(g.children, name) });

output(t);


