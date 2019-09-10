layout := { STRING letter; };

ds := DATASET([{'A'},{'B'},{'C'}], layout);

APPLY(ds, OUTPUT(DATASET([letter],layout),,'~temp::letters::'+letter, OVERWRITE));
