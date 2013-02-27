

string s1 := 'one' : stored('s1');
string s2 := 'five' : stored('s2');


d1 := dictionary([{'one'},{'two'},{'three'}], { string name }) : independent(many);
d2 := dictionary([{'one'},{'two'},{'three'},{'four'}], { string name }) : independent(many);
d3 := dictionary([{'one' => 1},{'one' => 2},{'two'=>2},{'four'=>4}], { string name => unsigned number }) : independent(many);

s1 in d1;
s2 not in d1;
s1 in d2;
s2 not in d2;
s1 in d3;
s2 not in d3;
