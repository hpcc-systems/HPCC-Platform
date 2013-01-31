IMPORT java;

integer add1(integer val) := IMPORT(java, 'JavaCat.add1:(I)I');
string add2(string val) := IMPORT(java, 'JavaCat.add2:(Ljava/lang/String;)Ljava/lang/String;');
string add3(varstring val) := IMPORT(java, 'JavaCat.add2:(Ljava/lang/String;)Ljava/lang/String;');
utf8 add4(utf8 val) := IMPORT(java, 'JavaCat.add2:(Ljava/lang/String;)Ljava/lang/String;');
unicode add5(unicode val) := IMPORT(java, 'JavaCat.add2:(Ljava/lang/String;)Ljava/lang/String;');
integer testThrow(integer p) := IMPORT(java, 'JavaCat.testThrow:(I)I');

string addChar(string c) := IMPORT(java, 'JavaCat.addChar:(C)C');
string cat(string s1, string s2) := IMPORT(java, 'JavaCat.cat:(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;');
data testData(data indata) := IMPORT(java, 'JavaCat.testData:([B)[B');

add1(10);
add2('Hello');
add3('World');
add4(U'Leovenaðes');
add5(U'Стоял');
addChar('A');

cat('Hello', ' world');
// Can't catch an expression(only a dataset)
d := dataset([{ 1, '' }], { integer a, string m} ) : stored('nofold');

d t := transform
  self.a := FAILCODE;
  self.m := FAILMESSAGE;
  self := [];
end;

catch(d(testThrow(a) = a), onfail(t));
testData(d'aa');

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
