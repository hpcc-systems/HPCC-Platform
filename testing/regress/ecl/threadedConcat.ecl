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

numRecords := 1000000;

r1 := { unsigned id; };

createIds(unsigned n, unsigned delta) := NOFOLD(DATASET(n, TRANSFORM(r1, SELF.id := COUNTER + delta)));

x(unsigned i) := createIds(numRecords, i);

combineDs(DATASET(r1) x1, DATASET(r1) x2, DATASET(r1) x3, DATASET(r1) x4, DATASET(r1) x5, DATASET(r1) x6) :=
     x1 + x2 + x3 + x4 + x5 + x6;

y(unsigned i) := combineDs(x(i), x(i+1000), x(i+2000), x(i+3000), x(i+4000), x(i+5000));

//z(unsigned i) := combineDs(y(i), y(i+8), y(i+16), y(i+24), y(i+32), y(i+40));

count(y(1));  // should be numRecords * 6
