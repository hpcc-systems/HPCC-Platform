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

//HOIST(dataset({unsigned i}) ds) := NOFOLD(SORT(NOFOLD(ds), i));
HOIST( ds) := MACRO
//NOFOLD(SORT(NOFOLD(ds), i))
//NOFOLD(ds)
ds
ENDMACRO;

mkRow(unsigned value) := TRANSFORM({ unsigned i }, SKIP(value = 1000000);   SELF.i := value);

dsOuter  := HOIST(DATASET(3, mkRow(COUNTER)));
dsInner1 := HOIST(DATASET(3, mkRow(COUNTER+10)));
dsInner2 := HOIST(DATASET(3, mkRow(COUNTER+20)));

innerSum1(unsigned x) := SUM(dsInner1, i * x);
outerSum1(unsigned x) := SUM(dsOuter, x * innerSum1(i));
innerSum2(unsigned x) := SUM(dsInner2, x * outerSum1(i));
outerSum2 := SUM(dsOuter, innerSum2(i));

sequential(
output(innerSum1(1));   // 36
output(outerSum1(1));   // 36 * 6 = 216
output(innerSum2(1));   // 216 * 22 * 3 = 14256
output(outerSum2);      // 14256 * 6 = 85536
);
