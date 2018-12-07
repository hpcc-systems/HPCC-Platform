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

#option ('targetClusterType', 'roxie');

//HOIST(dataset({unsigned i}) ds) := NOFOLD(SORT(NOFOLD(ds), i));
HOIST( ds) := MACRO
//NOFOLD(SORT(NOFOLD(ds), i))
NOFOLD(ds)
//ds
ENDMACRO;

//Very obscure code to trigger a problem in the code generator where an expression in a grandchild could theoretically
//be evaluated in the child query, but it would have a different meaning.
//In this case the SET(DATASET()) expression is dependent on LEFT(dsOuter).  That could be evaluated in the outer
//project or the nexsted project.  When the graph for the child query is processed the code needs to ensure that
//it is not hoisted.
//Two solutions:
//1. The child query is traversed, and for any operators that introduce the a selector that is already in scope all
//   child expressions are marked to prevent hoisting.  (Even if the expression also occurs outside.)
//2. Any expression that in introduces a selector that is already inscope is not transformed, but any selectors it
//   contains are remapped.

//The expression needs to be walked, and
//
//It generally requires LEFT rather than a dataset (e.g. complexhoist4) because when a dataset is resourced it
//has a unique id appended - which disambiguates it from the child dataset.

rec := { unsigned i };

mkRow(unsigned value) := TRANSFORM(rec, SKIP(value = 1000000);   SELF.i := value);

dsOuter  := HOIST(DATASET([1,2,3], rec, DISTRIBUTED));
dsInner := HOIST(DATASET([0,1,2], rec, DISTRIBUTED));


rec t1(unsigned x, rec l) := TRANSFORM,SKIP(x+l.i not in SET(DATASET(3, transform({ unsigned value}, SELF.value := COUNTER-1+(l.i-1)*2 )), value))
    SELF := l;
END;

filteredOuter(unsigned x) := PROJECT(dsOuter, t1(x, LEFT));
sumFilteredOuter(unsigned x) := AGGREGATE(filteredOuter(x), rec, transform(rec, SELF.i := (1<<(LEFT.i-1)) + RIGHT.i))[1].i;
sumInnerx() := TABLE(dsInner, {i, sumFilteredOuter(i)});
sumInner(unsigned x) := SUM(dsInner, x*sumFilteredOuter(i)<<i*3);
projectOuter() := PROJECT(dsOuter, transform(rec, SELF.i := sumInner(1<<(LEFT.i-1)*16)));
sumOuter() := SUM(NOFOLD(projectOuter()), i);

sequential(
//output(sumInner(1));   // 0b110111011 = 443
output(sumOuter());    // 0b11011101100000001101110110000000110111011 = 1,902,699,545,019
);
