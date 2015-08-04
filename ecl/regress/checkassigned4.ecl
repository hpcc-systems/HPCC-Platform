/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
record4 tranx1(record1 L1, record2 L2, record3 L3,integer i) := transform
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
end;
