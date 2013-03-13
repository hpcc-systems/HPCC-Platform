d := dataset([{1,2,3},{4,5,6}], {unsigned a, unsigned b, unsigned c});

i1 := index(d, { a => b, c}, 'my::index');
i2 := index(d, { a }, { b, c}, 'my::index');

buildindex(i1);
buildindex(i2);
i1[1];
i2[1];
