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

// test 4 level of access
// check a field is only assigned once

aa := dataset('aaa',{INTEGER a1; }, FLAT);

record1 := record
    integer i1;
    integer i2;
end;

record1 tranx1(integer i) := TRANSFORM
    SELF.i1 := i;
    SELF.i2 := 2;
    SELF.i2 := 3;
END;

record2 := record
    record1 r21;
    record1 r22;
end;

record2 tranx2(record1 r, integer i) := TRANSFORM
    SELF.r21.i1 := i;
    SELF.r21.i1 := i;
    SELF.r21 := r;
    SELF.r21 := r;
    SELF.r21.i1 := i; // X
    SELF.r22.i1 := i;
    SELF.r22.i1 := i; // X
    SELF.r22.i2 := 3;
END;
