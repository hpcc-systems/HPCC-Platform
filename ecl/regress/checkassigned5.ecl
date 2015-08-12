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
