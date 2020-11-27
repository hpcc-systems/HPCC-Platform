/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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

//class=embedded
//class=embedded-r
//class=3rdparty

IMPORT R;

integer add1(integer VAL) := EMBED(R)
VAL+1
ENDEMBED;

string cat(varstring what, string who) := EMBED(R)
paste(what,who)
ENDEMBED;

data testData(data val) := EMBED(R)
val[1] = val[2];
val;
ENDEMBED;

set of integer testSet(set of integer val) := EMBED(R)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of unsigned2 testSet0(set of unsigned2 val) := EMBED(R)
sort(val);
ENDEMBED;

set of string testSet2(set of string val) := EMBED(R)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of string testSet3(set of string8 val) := EMBED(R)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of varstring testSet4(set of varstring val) := EMBED(R)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of varstring8 testSet5(set of varstring8 val) := EMBED(R)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of boolean testSet6(set of boolean val) := EMBED(R)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of real4 testSet7(set of real4 val) := EMBED(R)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of real8 testSet8(set of real8 val) := EMBED(R)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of integer2 testSet9(set of integer2 val) := EMBED(R)
sort(val);
ENDEMBED;

mtcarsrec := RECORD
  real8     mpg;
  unsigned1 cyl;
  real8     disp;
  unsigned2 hp;
  real8     drat;
  real8     wt;
  real8     qsec;
  boolean   vs;
  boolean   am;
  unsigned1 gear;
  unsigned1 carb;
END;

DATASET(mtcarsrec) testDsOut(unsigned1 t) := EMBED(R)
mtcars;
ENDEMBED;

mtcarsrec testRecordOut(unsigned1 t) := EMBED(R)
mtcars[t,];
ENDEMBED;

mtcarsrec testRecordOut2(unsigned1 t) := EMBED(R)
list( 1,2,3,4,5,6,7,1,0,11,12);
ENDEMBED;

DATASET(mtcarsrec) testDsIn(DATASET(mtcarsrec) l) := EMBED(R)
l;
ENDEMBED;

add1(10);
cat('Hello', 'World');
testData(D'ab');
testSet([1,2,3]);
testSet0([30000,40000,50000]);
testSet2(['one','two','three']);
testSet3(['uno','dos','tre']);
testSet4(['un','deux','trois']);
testSet5(['ein','zwei','drei']);
testSet6([false,true,false,true]);
testSet7([1.1,2.2,3.3]);
testSet8([1.2,2.3,3.4]);
testSet9([-111,0,113]);

s1 :=DATASET(2500, TRANSFORM({ integer a }, SELF.a := add1(COUNTER)));
s2 :=DATASET(2500, TRANSFORM({ integer a }, SELF.a := add1(COUNTER/2)));
SUM(NOFOLD(s1 + s2), a);

s1b :=DATASET(2500, TRANSFORM({ integer a }, SELF.a := COUNTER+1));
s2b :=DATASET(2500, TRANSFORM({ integer a }, SELF.a := (COUNTER/2)+1));
SUM(NOFOLD(s1b + s2b), a);

testDsOut(1);
testRecordOut(1);
testRecordOut2(1);
testDsIn(SORT(testDsOut(1), mpg));

unsigned persistscope1(unsigned a) := EMBED(R: globalscope('yo'),persist('workunit'))
  b <- a + 1
  return(a)
ENDEMBED;

unsigned usepersistscope1(unsigned a) := EMBED(R: globalscope('yo'),persist('workunit'))
  return (a + b)
ENDEMBED;

unsigned persistscope2(unsigned a) := EMBED(R: globalscope('yi'),persist('workunit'))
  b <- a + 11
  return (a)
ENDEMBED;

unsigned usepersistscope2(unsigned a) := EMBED(R: globalscope('yi'),persist('workunit'))
  return(a + b)
ENDEMBED;

sequential(
  persistscope1(1),
  persistscope2(1),
  usepersistscope1(1),
  usepersistscope2(1)
);

