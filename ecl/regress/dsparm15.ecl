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

ds := dataset('ds', {integer m1; integer m2; }, THOR);

r1 := record integer n1; end;
r2 := record integer n2; integer n3 end;

dataset f(virtual dataset(r1) d) := d(n1 = 10);

dataset g(virtual dataset(r2) d) := d(n3 = max(f(d{n1:=n2}),n3));

ct := count(g(ds{n2:=m1; n3:=m2;}));

ct;
