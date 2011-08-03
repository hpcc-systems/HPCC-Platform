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

#option ('resourceConditionalActions', true);
#option ('targetClusterType', 'roxie');

//nothor

r := { string x{maxlength(256)}; };

trueSimple1 := true : stored('trueSimple1');
trueSimple2 := true : stored('trueSimple2');
trueSimple3 := true : stored('trueSimple3');

falseSimple1 := false : stored('falseSimple1');
falseSimple2 := false : stored('falseSimple2');
falseSimple3 := false : stored('falseSimple3');

complex1 := dedup(nofold(dataset([{'1'},{'b'},{'c'}], r)), x, all);
complex2 := dedup(nofold(dataset([{'2'},{'b'},{'c'}], r)), x, all);
complex3 := dedup(nofold(dataset([{'3'},{'b'},{'c'}], r)), x, all);

trueComplex1 := count(dedup(nofold(dataset([{'1'},{'b'},{'c'}], r)), x, all)) < 4;
trueComplex2 := count(dedup(nofold(dataset([{'2'},{'b'},{'c'}], r)), x, all)) < 4;
trueComplex3 := count(dedup(nofold(dataset([{'3'},{'b'},{'c'}], r)), x, all)) < 4;

falseComplex1 := count(dedup(nofold(dataset([{'1'},{'b'},{'x'}], r)), x, all)) > 4;
falseComplex2 := count(dedup(nofold(dataset([{'2'},{'b'},{'x'}], r)), x, all)) > 4;
falseComplex3 := count(dedup(nofold(dataset([{'3'},{'b'},{'x'}], r)), x, all)) > 4;

failComplex1 := count(limit(nofold(dataset([{'1'},{'b'},{'c'}], r)), 1, FAIL(99, 'Should not read this input 1'))) < 4;
failComplex2 := count(limit(nofold(dataset([{'2'},{'b'},{'c'}], r)), 1, FAIL(99, 'Should not read this input 2'))) < 4;
failComplex3 := count(limit(nofold(dataset([{'3'},{'b'},{'c'}], r)), 1, FAIL(99, 'Should not read this input 3'))) < 4;



if (trueSimple1, output(complex1, named('CorrectOutput1')));
if (falseSimple1, output(complex1, named('IncorrectOutput1')));
if (trueSimple2, output(complex1(x != '1'), named('CorrectOutput2')));


if (trueSimple3,
    parallel(
        output(complex2, named('CorrectOutput3')),
        output(complex3, named('CorrectOutput4'))),
    output(complex2, named('IncorrectOutput3')));

if (trueSimple1,
    if (falseSimple1,
        output(complex1, named('IncorrectOutput4')),
        output(complex3, named('CorrectOutput5'))),
    output(complex2, named('IncorrectOutput5')));


