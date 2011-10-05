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

r := {unsigned f1, unsigned f2, unsigned f3, unsigned f4 };

r t(unsigned a, unsigned b, unsigned c, unsigned d) := TRANSFORM
    SELF.f1 := a;
    SELF.f2 := b;
    SELF.f3 := c;
    SELF.f4 := d;
END;

ds := dataset([
        t(1,2,3,4),
        t(1,4,2,5),
        t(9,3,4,5),
        t(3,4,2,9)]);

simple := dedup(nofold(ds), f1);

osum := output(TABLE(simple, { s := sum(group, f1) }, f3));

x1 := when(simple, osum, parallel);

o1 := output(TABLE(x1, { f1 }));
o2 := output(TABLE(simple, { c := count(group) }, f3));
when(o1, o2, success);
