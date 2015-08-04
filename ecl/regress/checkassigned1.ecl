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

record2 tranx1(record1 L, integer i) := transform
        self.r1.i1 := 1;
    self.r1.i2 := 2;
    self.r2.i1 := 3;
    self.r2.i2 := i;
end;

record2 tranx2(record1 L, integer i) := transform
        self.r1 := L;
    self.r2 := L;
end;

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


