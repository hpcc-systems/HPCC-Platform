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
