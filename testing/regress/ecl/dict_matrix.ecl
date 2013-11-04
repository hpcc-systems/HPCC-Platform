// Using a dictionary to implement a sparse matrix

matrix := { integer4 X, integer4 Y => real8 V { default(0.0)} };
m1 := DICTIONARY([ { 1,1 => 1 }, { 2,2 => 1}, { 3,3 => 1 }],  matrix) : global;
m1;
output(m1);
output(dataset(m1));
