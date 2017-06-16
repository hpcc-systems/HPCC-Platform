/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

//noroxie   - roxie doesn't support spilling to disk with the results being read from a child graph, the spill file is too large for memory

//generate an error if a workunit spill is used
#option ('outputLimitMb', 1);

r := record
  integer i;
end;

r t(integer c) := transform
  SELF.i := c;
end;

ds1 := NOCOMBINE(dataset(1000000, t(COUNTER)));
ds2 := sort(ds1, i)(i != 5);

ds3 := DATASET(100, t(COUNTER));

f(dataset(r) inDs) := join(inDs, ds2, LEFT.i % 10 = RIGHT.i, t(LEFT.i+1));

l := loop(ds3, 3, f(ROWS(LEFT)));

sort(l, i);
