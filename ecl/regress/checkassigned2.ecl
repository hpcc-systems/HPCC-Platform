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

// this test errors

aa := dataset('aaa',{INTEGER a1; }, FLAT);

record1 := record
    integer i1;
    integer i2;
end;

record2 := record
    record1 r1;
    record1 r2;
end;

record3 := record
    integer i1;
    integer i2;
    record1 r1;
    record1 r2;
end;


record1 tranx0(integer i, integer j) := transform
    self.i1 := i;
    self.i2 := j;
end;

// should OK
record2 tranx1(record1 L, integer i) := transform
        self.r1.i1 := 1;
    self.r1.i2 := 2;
    self.r2.i1 := 3;
    self.r2.i2 := i;
end;


// should OK

record2 tranx2(record1 L, integer i) := transform
        self.r1 := L;
    self.r2 := L;
end;

// should OK
record3 tranx3(record1 L, integer i) := transform
    self := L;
        self.r1 := L;
    self.r2.i1 := 1;
    self.r2.i2 := 2;
end;


record3 tranx4(record1 L, integer i) := transform
    self := L;
        self.r1 := L;
    self.r2.i1 := 1;
    self.r2.i2 := i;
end;

record3 tranx5(integer i, integer j) := transform
    self.i1 := i;
    self.i2 := j;
        self.r1.i1 := i;
        self.r1.i2 := j;
    self.r2.i1 := i;
    self.r2.i2 := j;
end;

mytable := dataset([{1,2},{3,4}], record1);

normalizedStuff := normalize(mytable, LEFT.i1, tranx4(LEFT, COUNTER));

output(normalizedStuff);


