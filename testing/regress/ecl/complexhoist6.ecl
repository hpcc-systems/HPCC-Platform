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

HOIST( ds) := MACRO
NOFOLD(global(ds,few))          // causes a problem if isInlineTrivialDataset() walks to next item for no_fold
ENDMACRO;

rec := { unsigned i };

mkRow(unsigned value) := TRANSFORM(rec, SKIP(value = 1000000);   SELF.i := value);

dsOuter  := HOIST(DATASET([1,2,3], rec));
dsInner := HOIST(DATASET([0,1,2], rec));


rec t1(unsigned x, rec l) := TRANSFORM,SKIP(x+l.i not in SET(DATASET(3, transform({ unsigned value}, SELF.value := COUNTER-1+(l.i-1)*2 )), value))
    SELF := l;
END;

filteredOuter(unsigned x) := PROJECT(dsOuter, t1(x, LEFT));
sumFilteredOuter(unsigned x) := AGGREGATE(filteredOuter(x), rec, transform(rec, SELF.i := (1<<(LEFT.i-1)) + RIGHT.i))[1].i;
sumInnerx() := TABLE(dsInner, {i, sumFilteredOuter(i)});
sumInner(unsigned x) := SUM(dsInner, x*sumFilteredOuter(i)<<i*3);
projectOuter() := PROJECT(dsOuter, transform(rec, SELF.i := sumInner(1<<(LEFT.i-1)*16)));
sumOuter() := SUM(projectOuter(), i);

sequential(
output(sumInner(1));   // 0b110111011 = 443
output(sumOuter());    // 0b11011101100000001101110110000000110111011 = 1,902,699,545,019
);
