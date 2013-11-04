d := dataset([
 { 'A', 1 },
 { 'A', 4 },
 { 'A', 2 },
 { 'A', 3 },
 { 'B', 1 },
 { 'B', 3 },
 { 'B', 4 },
 { 'B', 2 },
 { 'B', 5 },
 { 'A', 5 }
], { string a, integer b });

SUBSORT(d, {a}, {b});
