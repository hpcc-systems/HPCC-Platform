ds6 := nofold(dataset([{5 => 'Richard'}], { integer id => string name }));
d6 := DICTIONARY(ds6);

5 in d6;
count(d6)=1;
