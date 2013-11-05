//nothor

IMPORT java;

integer add1(integer val) := IMPORT(java, 'JavaCat.add1:(I)I');
string add2(string val) := IMPORT(java, 'JavaCat.add2:(Ljava/lang/String;)Ljava/lang/String;');
string add3(varstring val) := IMPORT(java, 'JavaCat.add2:(Ljava/lang/String;)Ljava/lang/String;');
utf8 add4(utf8 val) := IMPORT(java, 'JavaCat.add2:(Ljava/lang/String;)Ljava/lang/String;');
unicode add5(unicode val) := IMPORT(java, 'JavaCat.add2:(Ljava/lang/String;)Ljava/lang/String;');

string addChar(string c) := IMPORT(java, 'JavaCat.addChar:(C)C');
string cat(string s1, string s2) := IMPORT(java, 'JavaCat.cat:(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;');
data testData(data indata) := IMPORT(java, 'JavaCat.testData:([B)[B');
integer testArrays(set of boolean b, set of integer2 s, set of integer4 i, set of real8 d) := IMPORT(java, 'JavaCat.testArrays:([Z[S[I[D)I');
set of string testStringArray1(set of string s) := IMPORT(java, 'JavaCat.testStringArray:([Ljava/lang/String;)[Ljava/lang/String;');
set of varstring testStringArray2(set of varstring s) := IMPORT(java, 'JavaCat.testStringArray:([Ljava/lang/String;)[Ljava/lang/String;');
set of string8 testStringArray3(set of string8 s) := IMPORT(java, 'JavaCat.testStringArray:([Ljava/lang/String;)[Ljava/lang/String;');
set of varstring8 testStringArray4(set of varstring8 s) := IMPORT(java, 'JavaCat.testStringArray:([Ljava/lang/String;)[Ljava/lang/String;');
set of utf8 testStringArray5(set of utf8 s) := IMPORT(java, 'JavaCat.testStringArray:([Ljava/lang/String;)[Ljava/lang/String;');
set of unicode8 testStringArray6(set of unicode8 s) := IMPORT(java, 'JavaCat.testStringArray:([Ljava/lang/String;)[Ljava/lang/String;');
set of unicode testStringArray7(set of unicode s) := IMPORT(java, 'JavaCat.testStringArray:([Ljava/lang/String;)[Ljava/lang/String;');

add1(10);
add2('Hello');
add3('World');
add4(U'Leovenaðes');
add5(U'Стоял');
addChar('A');

cat('Hello', ' world');

testData(d'aa');
testArrays([true],[2,3],[4,5,6,7],[8.0,9.0]);
testArrays([],[],[],[]);
testStringArray1(['one', 'two', 'three']);
testStringArray2(['one', 'two', 'three']);
testStringArray3(['one', 'two', 'three']);
testStringArray4(['one', 'two', 'three']);
testStringArray5(['one', 'two', 'three']);
testStringArray6(['one', 'two', 'three']);
testStringArray7(['one', 'two', 'three']);

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
