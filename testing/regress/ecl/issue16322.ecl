

r1 := RECORD
    unsigned id;
END;

r2 := RECORD
    unsigned id;
    unsigned cnt;
    dataset(r1) child;
END;

ds := DATASET(10000, TRANSFORM(r2, SELF.id := COUNTER; SELF.cnt := COUNTER % 30; SELF.child := DATASET(COUNTER % 30, TRANSFORM(r1, SELF.id := COUNTER))));

r2 t(r2 l) := TRANSFORM
    d := DEDUP(l.child, id);
    d2 := d(id != count(d)+1);
    SELF.id := l.id;
    SELF.cnt := l.cnt - count(d2);
    SELF.child := d2;
END;

p := PROJECT(ds, t(LEFT),PARALLEL(10));

output(NOFOLD(p)(cnt != 0));

