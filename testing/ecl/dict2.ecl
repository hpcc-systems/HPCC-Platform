// Valid...
d1 := dictionary([{5 => 'Richard'}], { integer id => string name });
d2 := dictionary([{5,2 => 'Richard'}], { integer id1, integer id2 => string name });
d3 := dictionary([], { integer id => string name });
d4 := dictionary([], { integer id, string name });
d5 := dictionary([{5, 'Richard'}], { integer id, string name });

d1n := nofold(d1);
d2n := nofold(d2);
d3n := nofold(d3);
d4n := nofold(d4);
d5n := nofold(d5);

d1[5].name = 'Richard';
5 in d1;
d1[1].name = '';
1 not in d1;
count(d1) = 1;
d1n[5].name = 'Richard';
5 in d1n;
d1n[1].name = '';
1 not in d1n;
count(d1n) = 1;

d2[5,2].name = 'Richard';
d2[5,1].name = '';
ROW({5,2}, { integer id1, integer id2} ) in d2;
ROW({5,1}, { integer id1, integer id2} ) not in d2;
count(d2) = 1;
d2n[5,2].name = 'Richard';
d2n[5,1].name = '';
ROW({5,2}, { integer id1, integer id2} ) in d2n;
ROW({5,1}, { integer id1, integer id2} ) not in d2n;
count(d2n) = 1;

5 not in d3;
count(d3) = 0;
5 not in d3n;
count(d3n) = 0;

ds6 := dataset([{5, 'Richard'}], { integer id, string name });
d6 := DICTIONARY(ds6, { id => name });

d7 := IF(nofold(true), d5);
d7n := IF(nofold(true), d5, d4);
ROW({5,'Richard'}, { integer id, string name} ) in d7;
ROW({5,'Richard'}, { integer id, string name} ) in d7n;
ROW({52,'Richard'}, { integer id, string name} ) not in d7;
ROW({51,'Richard'}, { integer id, string name} ) not in d7n;
