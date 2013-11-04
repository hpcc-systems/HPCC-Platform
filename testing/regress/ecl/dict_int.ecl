DataRec := RECORD
   INTEGER2   f;
   INTEGER2   v;
END;

d := DATASET([{1,2},{3,4}],DataRec);

i := DICTIONARY(d,{f => v});

OUTPUT(i[3]);
