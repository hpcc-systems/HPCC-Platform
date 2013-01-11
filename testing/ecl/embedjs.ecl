IMPORT javascript;

javascript.Language.syntaxcheck('1+2');

integer add1(integer val) := EMBED(javascript) val+1; ENDEMBED;
string add2(string val) := EMBED(javascript) val+'1'; ENDEMBED;
string add3(varstring val) := EMBED(javascript) val+'1'; ENDEMBED;

add1(10);
add2('Hello');
add3('World');

s1 :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := add1(COUNTER)));
s2 :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := add1(COUNTER/2)));
SUM(NOFOLD(s1 & s2), a);

s1a :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (integer) add2((STRING)COUNTER)));
s2a :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (integer) add3((STRING)(COUNTER/2))));
SUM(NOFOLD(s1a & s2a), a);

s1b :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := COUNTER+1));
s2b :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (COUNTER/2)+1));
SUM(NOFOLD(s1b & s2b), a);

s1c :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (integer) ((STRING) COUNTER + '1')));
s2c :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (integer) ((STRING)(COUNTER/2) + '1')));
SUM(NOFOLD(s1c & s2c), a);
