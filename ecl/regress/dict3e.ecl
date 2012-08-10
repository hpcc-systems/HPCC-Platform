// Some invalid dictionary operations...

d1a := dictionary([{1 => 'Richard'}], { integer id, string name });  // mismatch payload position
d1b := dictionary([{1 => 2, 'Richard'}], { integer id1, integer id2 => string name }); // mismatch payload position
d1c := dictionary([{1, 2, 'Richard'}], { integer id1, integer id2 => string name }); // mismatch payload position

d1 := dictionary([{5 => 'Richard'}], { integer id => string name });
'5' in d1;  // wrong type
