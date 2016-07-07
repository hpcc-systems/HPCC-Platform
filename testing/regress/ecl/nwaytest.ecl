

rec := RECORD
 unsigned key;
 string l;
END;

input1 := DATASET([{1, 'a1'}, {2, 'a2'}], rec);
input2 := DATASET([{1, 'b1'}, {2, 'b2'}], rec);
input3 := DATASET([{1, 'c1'}, {2, 'c2'}], rec);

p2 := PROJECT(NOFOLD(input2), TRANSFORM(rec, SELF.l := 't_' + LEFT.l; SELF := LEFT), PARALLEL(4));

inputs := [input1, p2, input3];

rec dojoin(DATASET(rec) m) := TRANSFORM
 SELF.key := m[1].key;
 SELF.l := m[1].l + m[2].l + m[3].l;
END;



parentrec := RECORD
 unsigned key;
 DATASET(rec) children1;
 DATASET(rec) children2;
 DATASET(rec) joinres;
END;

inrec := RECORD
 unsigned key;
 unsigned which;
END;

parentrec trans(inrec l) := TRANSFORM
 r := RANGE(inputs, [l.which, 3]);
 SELF.children1 := r[l.which];
 SELF.children2 := inputs[l.which];
 SELF.joinres := JOIN(r, LEFT.key=RIGHT.key, dojoin(ROWS(LEFT)), SORTED(key));
 SELF := l;
END;

ds := DATASET([{1, 1}, {2, 2}, {2, 2}], inrec);
t1 := PROJECT(ds, trans(LEFT), PARALLEL(4));

OUTPUT(t1);

