rec := RECORD
 unsigned4 id;
END;

numRecs := 5;
ds1 := DATASET(numRecs, TRANSFORM(rec, SELF.id := 100+COUNTER));
ds2 := DATASET(numRecs, TRANSFORM(rec, SELF.id := 200+COUNTER));
ds3 := DATASET(numRecs, TRANSFORM(rec, SELF.id := 300+COUNTER));
ds4 := DATASET(numRecs, TRANSFORM(rec, SELF.id := 400+COUNTER));

rec loopFunc(DATASET(rec) loopin, unsigned c) := FUNCTION
  cs := CASE(c,
            1 => loopin,
            2 => ds2,
            3 => loopin,
            4 => loopin,
            5 => ds3,
            6 => ds4,
            7 => PROJECT(loopin, TRANSFORM(rec, SELF.id := LEFT.id + COUNTER)),
            8 => loopin,
            9 => loopin);
  cn := CHOOSEN(NOFOLD(cs), IF(c=9, 4, 5));
  sloopin := IF(c=9, CHOOSEN(loopin, 4), loopin);
  RETURN COMBINE(sloopin, cn, TRANSFORM(rec, SELF.id := LEFT.id + RIGHT.id), LOCAL);
END;
l := LOOP(ds1, 9, loopFunc(ROWS(LEFT), COUNTER));

OUTPUT(l);
