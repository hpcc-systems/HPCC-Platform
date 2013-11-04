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
 SELF := l;
END;

agg := NOFOLD(AGGREGATE(NOFOLD(bssort), baserec, mytr(LEFT, RIGHT), LEFT.id, FEW));
sagg := SORT(agg, id);
OUTPUT(sagg)
