IMPORT javascript;

/*
 This example illustrates and tests the use of embedded JavaScript
 */

// Scalar parameters and resuls

integer add1(integer val) := EMBED(javascript) val+1; ENDEMBED;
string add2(string val) := EMBED(javascript) val+'1'; ENDEMBED;
string add3(varstring val) := EMBED(javascript) val+'1'; ENDEMBED;
utf8 add4(utf8 val) := EMBED(javascript) val+'1'; ENDEMBED;
unicode add5(unicode val) := EMBED(javascript, U' val+\' at Oh là là Straße\';');

data testData(data val) := EMBED(javascript) val[0] = val[0] + 1; val; ENDEMBED;

// Sets

set of integer testSet(set of integer val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of unsigned2 testSet0(set of unsigned2 val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of string testSet2(set of string val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of string testSet3(set of string8 val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of varstring testSet4(set of varstring val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of varstring8 testSet5(set of varstring8 val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of boolean testSet6(set of boolean val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of real4 testSet7(set of real4 val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of real8 testSet8(set of real8 val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

set of integer2 testSet9(set of integer2 val) := EMBED(javascript)
t = val [1];
val[1] = val[2];
val[2] = t;
val;
ENDEMBED;

// Now run the tests...

add1(10);
add2('Hello');
add3('World');
add4(U'Oh là là Straße');
add5(U'Стоял');

add2('Oh là là Straße');  // Passing latin chars - should be untranslated

testdata(D'aa');
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
