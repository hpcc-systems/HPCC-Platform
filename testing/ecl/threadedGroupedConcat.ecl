/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

// Testing that threaded concat preserves grouping information

numRecords := 10000;

r1 := { unsigned id; unsigned gs };

createIds(unsigned n, unsigned groupsize) :=
   GROUP(
       DATASET(n,
               TRANSFORM(r1, SELF.id := (COUNTER+groupsize-1) / groupsize, SELF.gs := groupsize)
              ),
       id);

x(unsigned i) := createIds(numRecords, i);

combineDs(GROUPED DATASET(r1) x1, GROUPED DATASET(r1) x2, GROUPED DATASET(r1) x3, GROUPED DATASET(r1) x4, GROUPED DATASET(r1) x5, GROUPED DATASET(r1) x6) :=
     x1 + x2 + x3 + x4 + x5 + x6;

combineDsU(GROUPED DATASET(r1) x1, GROUPED DATASET(r1) x2, GROUPED DATASET(r1) x3, GROUPED DATASET(r1) x4, GROUPED DATASET(r1) x5, GROUPED DATASET(r1) x6) :=
     x1 & x2 & x3 & x4 & x5 & x6;


y := GROUP(combineDs(x(1), x(2), x(3), x(4), x(5), x(6)));
yU := GROUP(combineDsU(x(1), x(2), x(3), x(4), x(5), x(6)));

//y;
// These three counts should be the same
count(dedup(yU, RECORD));
count(dedup(y, RECORD));
count(dedup(y, RECORD, ALL));
