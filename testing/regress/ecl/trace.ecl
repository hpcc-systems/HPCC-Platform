

rec := RECORD
    UNSIGNED id;
END;

ds := NOFOLD(DATASET(10, transform(rec, SELF.id := COUNTER;)));
gds := GROUP(ds, id);
 
sequential(
  output(trace(ds));
  output(trace(ds,id=5,NAMED('ds5only')));
  output(trace(ds,keep(2),NAMED('keep2')));
  output(trace(ds,skip(3),keep(2),NAMED('skip3keep2')));
  output(trace(ds,sample(2),NAMED('sample2')));
  output(table(trace(gds,sample(2),NAMED('sample2')),{COUNT(GROUP)}));
);
