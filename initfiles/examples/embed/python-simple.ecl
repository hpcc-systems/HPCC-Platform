IMPORT Python3 AS Python;

/*
 This example illustrates and tests the use of embedded Python
 */

// Scalar parameters and resuls

INTEGER add1(INTEGER val) := EMBED(Python)
  val+1
ENDEMBED;

STRING add2(STRING val) := EMBED(Python)
  val+'1'
ENDEMBED;

STRING add3(VARSTRING val) := EMBED(Python)
  val+'1'
ENDEMBED;

UTF8 add4(UTF8 val) := EMBED(Python)
  val+'1'
ENDEMBED;

UNICODE add5(UNICODE val) := EMBED(Python)
  val+'1'
ENDEMBED;

UTF8 add6(UTF8 val) := EMBED(Python)
  return val+'1'
ENDEMBED;

UNICODE add7(UNICODE val) := EMBED(Python)
  return val+'1'
ENDEMBED;

DATA testData(DATA val) := EMBED(Python)
  val[0] = val[0] + 1
  return val
ENDEMBED;

// Sets in ECL map to Python lists

SET OF INTEGER testSet(SET OF INTEGER val) := EMBED(Python)
  return sorted(val)
ENDEMBED;

SET OF STRING testSet2(SET OF STRING val) := EMBED(Python)
  return sorted(val)
ENDEMBED;

SET OF STRING testSet3(SET OF STRING8 val) := EMBED(Python)
  return sorted(val)
ENDEMBED;

SET OF UTF8 testSet4(SET OF UTF8 val) := EMBED(Python)
  return sorted(val)
ENDEMBED;

SET OF VARSTRING testSet5(SET OF VARSTRING val) := EMBED(Python)
  return sorted(val)
ENDEMBED;

SET OF VARSTRING8 testSet6(SET OF VARSTRING8 val) := EMBED(Python)
  return sorted(val)
ENDEMBED;

SET OF UNICODE testSet7(SET OF UNICODE val) := EMBED(Python)
  return sorted(val)
ENDEMBED;

SET OF UNICODE8 testSet8(SET OF UNICODE8 val) := EMBED(Python)
  return sorted(val)
ENDEMBED;

SET OF DATA testSet9(SET OF DATA val) := EMBED(Python)
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