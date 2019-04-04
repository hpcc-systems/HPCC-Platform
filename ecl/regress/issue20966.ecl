DataRec := {UNSIGNED4 n};

ds1 := DATASET([7,3,5,1,9], DataRec);
ds2 := DATASET([6,2,8,4], DataRec);

sortedDS1 := SORT(NOFOLD(ds1), -n);
sortedDS2 := SORT(NOFOLD(ds2), -n);

res := MERGE([sortedDS1, sortedDS2], SORTED(-n));
OUTPUT(res, NAMED('res'));
