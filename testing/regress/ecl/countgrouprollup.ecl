

ds := DATASET([], { unsigned id; });

g := GROUP(NOFOLD(ds), id);

r := ROLLUP(g, GROUP, TRANSFORM({unsigned i}, SELF.i := COUNT(ROWS(LEFT))));

output(count(r));
