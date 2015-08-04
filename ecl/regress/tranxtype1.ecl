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
