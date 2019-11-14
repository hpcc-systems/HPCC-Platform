r := RECORD
  boolean b;
  UNICODE80 u;
END;

r2 := RECORD
  boolean b;
  UNICODE100 u;
END;

d := NOFOLD(DATASET([{true, 'Hello'}], r));

OUTPUT(PROJECT(d, TRANSFORM(r2, SELF := LEFT)));
