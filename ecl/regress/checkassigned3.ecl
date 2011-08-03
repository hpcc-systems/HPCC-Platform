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

// this should parse and compile without error

aa := dataset('aaa',{INTEGER a1; }, FLAT);

record1 := record
    integer i1;
    integer i2;
end;

record2 := record
    record1 r21;
    record1 r22;
end;

record3 := record
    integer i1;
    integer i2;
    record1 r1;
    record1 r1x;
    record2 r2;
    record2 r2x;
end;


/* remove any of the assignment should get errors */
record3 tranx1(record1 L1, record2 L2, integer i) := transform
    self := L1;

        self.r1.i1 := 1;
    self.r1.i2 := 2;
    self.r1x := L1;

    self.r2.r21 := L1;
    self.r2.r22 := L1;

    self.r2x := L2;
end;
