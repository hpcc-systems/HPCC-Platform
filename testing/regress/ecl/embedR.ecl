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

s1 :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := add1(COUNTER)));
s2 :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := add1(COUNTER/2)));
SUM(NOFOLD(s1 + s2), a);

s1b :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := COUNTER+1));
s2b :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (COUNTER/2)+1));
SUM(NOFOLD(s1b + s2b), a);

testDsOut(1);
testRecordOut(1);
testRecordOut2(1);
testDsIn(SORT(testDsOut(1), mpg));