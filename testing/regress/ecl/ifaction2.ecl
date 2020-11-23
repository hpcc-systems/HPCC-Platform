#option ('resourceConditionalActions', true);

d := NOFOLD(DATASET([{1},{2},{3}], { unsigned v; }));

spl1 := NOFOLD(d);
spl2 := NOFOLD(d(v=1));
spl3 := NOFOLD(d(v=2));

s2 := NOFOLD(true) : STORED('s2');
s3 := NOFOLD(false) : stored('s3');

unstopped := IF(s2, OUTPUT(spl1), OUTPUT(spl2));

stopped := IF(s3, unstopped, OUTPUT(spl3));

stopped;
