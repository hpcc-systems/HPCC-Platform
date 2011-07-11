/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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