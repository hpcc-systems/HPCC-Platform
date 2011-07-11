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
