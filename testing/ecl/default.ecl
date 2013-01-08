// Test that default field values are used as many places as I can think of

zero := nofold(0);

// 1. when outputting an out-of-range row

ds := DATASET([{ 'A' }], {string1 f {DEFAULT('B')}});

ds[0];      // should be constant folded
ds[zero];   // should NOT be

dsn := DATASET([], {string1 f {DEFAULT('B')}});
dsn[0];     // should be constant folded
dsn[zero];  // can also be

// 2. when looking up in a dictionary

dict := DICTIONARY([{ 1 => 'A' }], { unsigned1 v => string1 f {DEFAULT('B')}});

dict[0];      // Might one day be constant folded
dict[zero];   // should NOT be

dictn := DICTIONARY([], { unsigned1 v => string1 f {DEFAULT('B')}});   // Bit useless !

dictn[0];      // Might be constant folded
dictn[zero];   // Might be constant folded

// 3. When assigning self := []

ds t1 := TRANSFORM
  SELF := [];
END;

PROJECT(ds, t1);

// 4. In the default rows for outer joins

ds t2(ds L, dsn R) := TRANSFORM
  SELF.f := R.f;
END;

j := JOIN(ds, dsn, left.f=right.f, t2(LEFT, RIGHT),  LEFT OUTER);

j[1].f;
