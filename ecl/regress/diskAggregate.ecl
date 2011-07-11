baserec := RECORD
    unsigned id;
    string value;
    unsigned order;
END;

baseset := DATASET([
 {1, 'one', 1},
 {2, 'four', 2},
 {3, 'seven', 3},
 {1, 'two', 4},
 {2, 'five', 5},
 {3, 'eight', 6},
 {1, 'three', 7},
 {2, 'six', 8},
 {3, 'nine', 9}
 ], baserec);

bssort := SORT(baseset, order);

baserec mytr(baserec l, baserec r) := TRANSFORM
 SELF.value := r.value + (l.value+', ');
 SELF.id := l.id; // seems to be losing this field value
 SELF := l;
END;

o1 := OUTPUT(bssort, , '~bssort', OVERWRITE);

d1 := DATASET('~bssort', baserec, FLAT);
agg := NOFOLD(AGGREGATE(d1, baserec, mytr(LEFT, RIGHT), LEFT.id, FEW));

SEQUENTIAL(
o1,
//OUTPUT(d1)
OUTPUT(agg)
);

//OUTPUT(baseset);
