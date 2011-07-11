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

o1 := OUTPUT(bssort, , '~bssort', OVERWRITE);

baserecfp := RECORD
 baserec;
 unsigned8 __filepos {virtual(fileposition)};
END;

d1 := DATASET('~bssort', baserecfp, FLAT);

irec := RECORD
 d1.id;
 string10 v := d1.value;
 unsigned filepos := d1.__filepos;
END;

i := INDEX(d1, irec, '~bsidx');

outrec := RECORD
  unsigned id;
 string ov; 
END;

outrec mytr(irec l, outrec r) := TRANSFORM
 SELF.ov := r.ov + (l.v+', ');
SELF.id := l.id;
END;

bi := BUILD(i, OVERWRITE);

agg := NOFOLD(AGGREGATE(i, outrec, mytr(LEFT, RIGHT)));

SEQUENTIAL(
o1,
bi,
//OUTPUT(i),
//OUTPUT(d1),
OUTPUT(agg),
OUTPUT('done')
);

//OUTPUT(baseset);
