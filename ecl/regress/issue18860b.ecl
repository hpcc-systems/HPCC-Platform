rhsRec := RECORD
 unsigned4 key;
 string10 f1;
 string10 f2;
 unsigned extra;
END;

lhsRec := RECORD
 unsigned4 lhsKey;
END;

rhs := DATASET([{1, 'a', 'b', 1}, {1, 'a2', 'b2', 2}, {1, 'a3', 'b3', 3}, {2, 'c', 'd', 3}, {3, 'e', 'f', 5}], rhsRec);
rhsDs := DATASET('~rhsDs', rhsRec, FLAT);

lhs := DATASET([{1}], lhsRec);

i := INDEX(rhsDs, {key} , { rhsDs} , '~rhsDs.idx', FILEPOSITION(FALSE));

rhsRec doJoinTrans(lhsRec l, rhsRec r) := TRANSFORM
 SELF.key := l.lhsKey;
 SELF := r;
END;

j8 := JOIN(lhs, rhsDs, LEFT.lhsKey=RIGHT.key AND RIGHT.f2 != 'b2', doJoinTrans(LEFT, RIGHT), KEYED(i), KEEP(2));

SEQUENTIAL(
 OUTPUT(rhs, , '~rhsDs', OVERWRITE);
 BUILD(i, OVERWRITE);
 OUTPUT(j8);
);
