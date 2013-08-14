//nothor

IMPORT Python;

Python.Language.syntaxcheck('1+2');

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

s1 :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := add1(COUNTER)));
s2 :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := add1(COUNTER/2)));
 SUM(NOFOLD(s1 + s2), a);

s1a :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (integer) add2((STRING)COUNTER)));
s2a :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (integer) add3((STRING)(COUNTER/2))));
 SUM(NOFOLD(s1a + s2a), a);

s1b :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := COUNTER+1));
s2b :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (COUNTER/2)+1));
 SUM(NOFOLD(s1b + s2b), a);

s1c :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (integer) ((STRING) COUNTER + '1')));
s2c :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (integer) ((STRING)(COUNTER/2) + '1')));
 SUM(NOFOLD(s1c + s2c), a);
