IMPORT R;

integer add1(integer val) := EMBED(R)
val+1
ENDEMBED;

string cat(varstring what, string who) := EMBED(R)
paste(what,who)
ENDEMBED;

add1(10);
cat('Hello', 'World');

s1 :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := add1(COUNTER)));
s2 :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := add1(COUNTER/2)));
SUM(NOFOLD(s1 + s2), a);

s1b :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := COUNTER+1));
s2b :=DATASET(250000, TRANSFORM({ integer a }, SELF.a := (COUNTER/2)+1));
SUM(NOFOLD(s1b + s2b), a);
