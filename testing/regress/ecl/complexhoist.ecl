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

dsOuter := HOIST(DATASET([1,2,3,4,5,6,7,8,9], { unsigned i }));
dsInner1 := HOIST(DATASET([11,12,13,14,15,16,17,18,19], { unsigned i }));
dsInner2 := HOIST(DATASET([21,22,23,24,25,26,27,28,29], { unsigned i }));

innerSum1(unsigned x) := SUM(dsInner1, i * x);
outerSum1(unsigned x) := SUM(dsOuter, x * innerSum1(i));
innerSum2(unsigned x) := SUM(dsInner2, i * outerSum1(i));
outerSum2 := SUM(dsOuter, innerSum2(i));

sequential(
output(innerSum1(1));
output(outerSum1(1));
output(innerSum2(1));
output(outerSum2);
);
