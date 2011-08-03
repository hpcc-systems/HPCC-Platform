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

record1 := record
    integer i;
    boolean b;
    string1 s;
end;

// simple assign
record1 tranx1(integer i, STRING s, boolean b) := transform
    self.i := i;
    self.b := s;
    self.s := b;
end;

record1x := record
    integer i;
// note the types for b ans s are different from these in record1.
    string1 b;
    boolean s;
end;

// assign all
record1 tranx2(record1x r) := transform
    self := r;
end;

// child dataset
record2 := record
    record1 r21;
    record1 r22;
end;

record2 tranx3(record1x r, integer i, string s, boolean b) := transform
    self.r21 := r;
    self.r22.i := i;
    self.r22.s := b;
    self.r22.b := s;
end;
