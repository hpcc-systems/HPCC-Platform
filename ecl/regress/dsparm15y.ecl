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

r1 := record integer m1; end;
r2 := record integer m1; integer m2 end;

dataset f(virtual dataset(r1) d) := d(m1 = 10);

dataset g(virtual dataset(r2) d) := d(m2 = max(f(d),m2));

ct := count(g(ds));

ct;
