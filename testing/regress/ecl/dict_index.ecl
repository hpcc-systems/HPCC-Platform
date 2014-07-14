//nothor
//noroxie
d := dataset(
  [{ '1', '2',
    [{ '3' => '4'},
     { 'a' => 'A'},
     { 'b' => 'B'},
     { 'c' => 'C'},
     { 'd' => 'D'},
     { 'e' => 'E'},
     { 'f' => 'F'}],
     '5'
  }],
   { string1 a, string1 b, dictionary({string1 c1=>string1 c2}) c, string1 d });

i := index(d, {a,b}, {c,d}, 'test::dict');

recordof(d) t(d L, i R) := TRANSFORM
  self := R;
END;

sequential(
  buildindex(i, OVERWRITE);
  i(KEYED(a='1'))[1].c['3'].c2;
  OUTPUT(join(d, i, KEYED(LEFT.a = RIGHT.a) AND 'c' IN RIGHT.c, t(left, right)));
  OUTPUT(join(d, i, KEYED(LEFT.a = RIGHT.a) AND 'c' NOT IN RIGHT.c, t(left, right),LEFT OUTER));
  OUTPUT(join(d, i, KEYED(LEFT.a = RIGHT.a) AND (RIGHT.c + left.c)['f'].c2='F', t(left, right)))
);
