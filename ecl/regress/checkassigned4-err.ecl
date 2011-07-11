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

record4 := record
    integer i1;
    integer i2;
    record1 r1; 
    record1 r1x;
    record2 r2;
    record2 r2x;
    record3 r3;
    record3 r3x;
end;    

/* leave any of the assignment out should cause errors */
/* total 42 fields plus on empty transform error: 43 errors */
record4 tranx1(record1 L1, record2 L2, record3 L3,integer i) := transform
/*
    self := L1;

        self.r1.i1 := 1;    
    self.r1.i2 := 2;
    self.r1x := L1;

    // level 2
    self.r2.r21 := L1;
    self.r2.r22 := L1;

    self.r2x := L2;

    // level 3
        self.r3 := L3;    
    self.r3x.i1 := 1;
    self.r3x.i2 := 2;
    self.r3x.r1 := L1;
    self.r3x.r1x.i1 := 1;
    self.r3x.r1x.i2 := 1;
    self.r3x.r2 := L2;
    self.r3x.r2x.r21 := L1;
    self.r3x.r2x.r22.i1 := 1;
    self.r3x.r2x.r22.i2 := 2;
*/
end;
