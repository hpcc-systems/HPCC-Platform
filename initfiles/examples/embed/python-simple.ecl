IMPORT Python;

/*
 This example illustrates and tests the use of embedded Python
 */

// Scalar parameters and resuls

integer add1(integer val) := EMBED(Python)
val+1
ENDEMBED;

string add2(string val) := EMBED(Python)
val+'1'
ENDEMBED;

string add3(varstring val) := EMBED(Python)
val+'1'
ENDEMBED;

utf8 add4(utf8 val) := EMBED(Python)
val+'1'
ENDEMBED;

unicode add5(unicode val) := EMBED(Python)
val+'1'
ENDEMBED;

utf8 add6(utf8 val) := EMBED(Python)
return val+'1'
ENDEMBED;

unicode add7(unicode val) := EMBED(Python)
return val+'1'
ENDEMBED;

data testData(data val) := EMBED(Python)
val[0] = val[0] + 1
return val
ENDEMBED;

// Sets in ECL map to Python lists

set of integer testSet(set of integer val) := EMBED(Python)
return sorted(val)
ENDEMBED;

set of string testSet2(set of string val) := EMBED(Python)
return sorted(val)
ENDEMBED;

set of string testSet3(set of string8 val) := EMBED(Python)
return sorted(val)
ENDEMBED;

set of utf8 testSet4(set of utf8 val) := EMBED(Python)
return sorted(val)
ENDEMBED;

set of varstring testSet5(set of varstring val) := EMBED(Python)
return sorted(val)
ENDEMBED;

set of varstring8 testSet6(set of varstring8 val) := EMBED(Python)
return sorted(val)
ENDEMBED;

set of unicode testSet7(set of unicode val) := EMBED(Python)
return sorted(val)
ENDEMBED;

set of unicode8 testSet8(set of unicode8 val) := EMBED(Python)
return sorted(val)
ENDEMBED;

set of data testSet9(set of data val) := EMBED(Python)
return val
ENDEMBED;

// Now run the tests

add1(10);
add2('Hello');
add3('World');
add4(U'Oh là là Straße');
add5(U'Стоял');
add6(U'Oh là là Straße');
add7(U'Стоял');

add2('Oh là là Straße');  // Passing latin chars - should be untranslated

testData(D'aa');
testSet([1,3,2]);
testSet2(['red','green','yellow']);
testSet3(['one','two','three']);
testSet4([U'Oh', U'là', U'Straße']);
testSet5(['Un','Deux','Trois']);
testSet6(['Uno','Dos','Tre']);
testSet7([U'On', U'der', U'Straße']);
testSet8([U'Aus', U'zum', U'Straße']);
testSet9([D'Aus', D'zum', D'Strade']);