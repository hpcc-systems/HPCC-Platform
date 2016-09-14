r  := { INTEGER y };

d := DATASET(1, TRANSFORM({ r x },   SELF.x.y := COUNTER));

d2 := DATASET(1, TRANSFORM({ r x }, SELF.x := d[1].x, SELF.x.y := COUNTER));

OUTPUT(d2);