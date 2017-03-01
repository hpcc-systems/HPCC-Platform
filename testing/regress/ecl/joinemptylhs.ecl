lhsRec := RECORD
 string lhsstr := '';
 boolean abool;
end;

rhsRec := RECORD
 string5 rhsstr := '';
 unsigned4 auint;
end;

lhs := DATASET(NOFOLD(0), TRANSFORM(lhsRec, SELF.lhsstr := (string)COUNTER, SELF.abool := false));
rhs := DATASET(1, TRANSFORM(rhsRec, SELF.rhsstr := (string)COUNTER, SELF.auint := 1), DISTRIBUTED);

j := JOIN(NOFOLD(lhs), NOFOLD(rhs), LEFT.lhsstr=RIGHT.rhsstr);

SEQUENTIAL(
 COUNT(NOFOLD(j));
);
