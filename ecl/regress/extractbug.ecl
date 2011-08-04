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


