#option ('targetClusterType', 'hthor');

boolean b := false : stored('b');

d := nofold(sort(nofold(dataset([{'hello'}], {string a})),a));

d(b);
d(a = 'fred');
