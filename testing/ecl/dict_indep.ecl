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
   { string1 a, string1 b, dictionary({string1 c1=>string1 c2}) c, string1 d }) : independent;

d[1].c['c'].c2;
