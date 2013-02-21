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

sequential(
  buildindex(i, OVERWRITE);
  i(KEYED(a='1'))[1].c['3'].c2;
);
