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

C := 5 : stored('C');

r := record
    unsigned i;
end;

r t1(unsigned value) := transform
    SELF.i := value * 10;
end;

r t2() := transform
    SELF.i := 10;
end;

// zero
output('Zero Rows');
ds := DATASET(0, t1(COUNTER));
output(ds);

// plain
output('Constant, 10');
ds10 := DATASET(10, t1(COUNTER));
output(ds10);

// expr
output('Constant expression, 50');
ds50 := DATASET(5 * 10, t1(COUNTER));
output(ds50);

// variable
output('Variable 5, row constant');
ds5 := DATASET(C, t2());
output(ds5);

// distributed
output('Distributed 10');
dsd := DATASET(10, t1(COUNTER), DISTRIBUTED);
output(dsd);
