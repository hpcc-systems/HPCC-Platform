ds1 := DATASET(['Should not appear'], {STRING msg});
ds2 := DATASET(['Should not appear either'], {STRING msg});
ds3 := DATASET(['Should appear'],{STRING msg});

whenClause := OUTPUT(ds1);

falsearm := WHEN(ds2, whenClause, SUCCESS);
truearm := ds3;

OUTPUT(IF(nofold(true), truearm, falsearm));
