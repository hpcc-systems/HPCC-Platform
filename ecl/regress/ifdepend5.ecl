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

r := { string x{maxlength(256)}; };

trueSimple1 := true : stored('trueSimple1');
trueSimple2 := true : stored('trueSimple2');
trueSimple3 := true : stored('trueSimple3');

falseSimple1 := false : stored('falseSimple1');
falseSimple2 := false : stored('falseSimple2');
falseSimple3 := false : stored('falseSimple3');

trueComplex1 := count(dedup(nofold(dataset([{'1'},{'b'},{'c'}], r)), x, all)) < 4;
trueComplex2 := count(dedup(nofold(dataset([{'2'},{'b'},{'c'}], r)), x, all)) < 4;
trueComplex3 := count(dedup(nofold(dataset([{'3'},{'b'},{'c'}], r)), x, all)) < 4;

falseComplex1 := count(dedup(nofold(dataset([{'1'},{'b'},{'x'}], r)), x, all)) > 4;
falseComplex2 := count(dedup(nofold(dataset([{'2'},{'b'},{'x'}], r)), x, all)) > 4;
falseComplex3 := count(dedup(nofold(dataset([{'3'},{'b'},{'x'}], r)), x, all)) > 4;

failComplex1 := count(limit(nofold(dataset([{'1'},{'b'},{'c'}], r)), 1, FAIL(99, 'Should not read this input 1'))) < 4;
failComplex2 := count(limit(nofold(dataset([{'2'},{'b'},{'c'}], r)), 1, FAIL(99, 'Should not read this input 2'))) < 4;
failComplex3 := count(limit(nofold(dataset([{'3'},{'b'},{'c'}], r)), 1, FAIL(99, 'Should not read this input 3'))) < 4;



goodInput := dataset(['Correct input chosen','Success'], r);
badInput := dataset(['Wrong input chosen','Failure'], r);


sequential
(
//  output(IF(trueSimple1, goodInput, badInput));

//Check that dependencies within the same subgraph work correctly
    output(IF(trueComplex1, goodInput, nofold(IF(failComplex1, goodInput, badInput))));
    output(IF(falseComplex1, nofold(IF(failComplex1, badInput, goodInput)), goodInput));

//That absence of nofold doesn't mess things up.
//  output(IF(trueComplex1, goodInput, IF(failComplex1, goodInput, badInput)));
//  output(IF(falseComplex1, IF(failComplex1, badInput, goodInput), badInput));

//Some simple tests
//  output(IF(trueComplex1 OR failComplex1, goodInput, badInput));
//  output(IF(falseComplex1 AND failComplex1, badInput, goodInput));

//Last item
    output('Done')
);