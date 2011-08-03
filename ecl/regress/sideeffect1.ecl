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

#option ('targetClusterType', 'roxie');

//testsToRun := [7,8,9];
testsToRun := [1,2,3,4,5,6,7,8,9];
run(unsigned4 x) := x in testsToRun;
where() := macro
' line(' + (string)__LINE__ + ')'
endmacro;

falseValue := false : stored('falseValue');
trueValue := true : stored('trueValue');

rec := { unsigned id; };
rec2 := { unsigned id1; unsigned id2; };
rec3 := { unsigned id; dataset(rec) ids{maxcount(100)}; };
ds := dataset([1,2,3,4,5,6], rec);
alwaysFail(unsigned line) := IF(falseValue, ds, fail(rec, 99, 'This failure should never appear (line '+(string)line+')'));
alwaysFailValue(unsigned line) := alwaysFail(line)[2].id;

ds0 := dataset([4,5,6,7], rec);

//test1: ensure that dependencies are only evaluated if the condition is met.
ds1 := IF(falseValue, ds0(id != alwaysFailValue(__LINE__)));
o1 := output(ds1);

//Test 2 ensure that if() conditions aren't combined into an IF (same else condition - null dataset)
ds2a := IF(alwaysFailValue(__LINE__) = 3, ds0);
ds2b := IF(falseValue, ds2a);
o2 := output(ds2b);

//Test 3 the whole project should get folded away - but the dataset may have been hoisted already
in2 := dataset([{1,2},{3,4},{5,6}], rec2) : stored('in2');

rec2 t1(rec2 l) := transform
    self.id1 := l.id1;
    self.id2 := sort(dataset([l.id2], rec) + alwaysFail(__LINE__), -id)[3].id;
end;

ds3 := project(in2, t1(LEFT));
o3 := output(count(ds3));

//Test4.  The child query contains a subexpression that should be evaluated globally, but how lazy should the evaluation be?
rec3 t2(rec2 l) := transform
    self.id := l.id1;
    self.ids := IF(l.id1 <> 2, dataset([1,l.id1,l.id2,4],rec), fail(rec, 'This should never be hoisted' + where()));
end;
o4 := output(project(in2, t2(LEFT)));

//Test5.  Similar, but evaluation is hidden more
rec3 t5(rec2 l) := transform
    self.id := l.id1;
    self.ids := IF(l.id1 <> 2, dataset([1,l.id1,l.id2,4],rec), if(trueValue, fail(rec, 'This should never be hoisted - or at least not evaluated' + where())));
end;
o5 := output(project(in2, t5(LEFT)));

//Test6.  Similar, but evaluation is hidden more, and a cse
rec3 t6(rec2 l) := transform
    failIds := if(trueValue, fail(rec, 'This should never be hoisted - or at least not evaluated' + where()));
    self.id := IF(l.id1 <> 4, dataset([1,l.id2,l.id1,4],rec), failIds)[2].id;
    self.ids := IF(l.id1 <> 2, dataset([1,l.id1,l.id2,4],rec), failIds);
end;
o6 := output(project(in2, t6(LEFT)));

//Test 7 ensure that if() conditions aren't combined into a case (different else conditions)
ds7a := IF(alwaysFailValue(__LINE__) = 3, ds0);
ds7b := IF(trueValue, dataset([5,6],rec), ds7a);
o7 := output(ds7b);

unsigned4 getNextTicker(unsigned x) :=
BEGINC++
static unsigned myTicker;
#option pure
#body
    return ++myTicker;
ENDC++;

//Test8.  An attempt to ensure that invariant datasets are only evaluated once...  Even if conditional
rec3 t8(rec2 l) := transform
    someExpensiveOperation := project(nofold(dataset([999,1000],rec)), transform(rec, self.id := getNextTicker(left.id)));
    self.id := IF(l.id1 = 4, dataset([1,l.id2,l.id1,4],rec), someExpensiveOperation)[2].id;
    self.ids := IF(l.id1 = 2, dataset([1,l.id1,l.id2,4],rec), someExpensiveOperation);
end;
//Value will be 6 if the expression isn't hoisted, 2 if it is
o8 := output(IF(project(nofold(in2), t8(LEFT))[3].id=2, 'success', 'failure'));

dataset(rec) doThrowException(const varstring x) :=
BEGINC++
#option pure
    rtlFail(999, x);
ENDC++;

//Test9.  Similar to test 6, but hiding the fact that the exception is being thrown
rec3 t9(rec2 l) := transform
    failIds := if(trueValue, doThrowException('This should never be hoisted - or at least not evaluated' + where()));
    self.id := IF(l.id1 <> 4, dataset([1,l.id2,l.id1,4],rec), failIds)[2].id;
    self.ids := IF(l.id1 <> 2, dataset([1,l.id1,l.id2,4],rec), failIds);
end;
o9 := output(project(in2, t9(LEFT)));

//Want to try and ensure that complex loop invariant code is extracted out, but it still isn't evaluated unless it is actually going to be used.

sequential(
    if (run(1),o1),
    if (run(2),o2),
    if (run(3),o3),
    if (run(4),o4),
    if (run(5),o5),
    if (run(6),o6),
    if (run(7),o7),
    if (run(8),o8),
    if (run(9),o9),
    output('done')
);


