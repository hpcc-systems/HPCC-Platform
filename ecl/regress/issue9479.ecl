rec := RECORD
 unsigned4 key;
 unsigned4 aux;
 STRING50 payload;
END;

ds := DATASET([{1,81,''}, {1,82,''}, {1,83,''}, {2,91,''}], rec);

i := INDEX(ds, {key}, {aux, payload}, '~aindex');

lhs := TABLE(ds, { key });

rec joinFunc(lhs l, i r) := TRANSFORM
  SELF := l;
  SELF := r;
END;

SEQUENTIAL(
BUILDINDEX(SORTED(ds, key, payload), { key }, { aux, payload }, '~aindex', OVERWRITE),

// Fails with code generator error "Dataset 'right' does not contain expression 'key'"
// OUTPUT(JOIN(lhs, i, LEFT.key = RIGHT.key, joinFunc(LEFT, RIGHT), LIMIT(2), ONFAIL(TRANSFORM(rec, SELF.payload := '***FAILED***', SELF.key := RIGHT.key; SELF.aux := RIGHT.aux))))
// Works, if 'key' is extracted from LEFT instead. Also works if hThor is targeted instead.
OUTPUT(JOIN(lhs, i, LEFT.key = RIGHT.key, joinFunc(LEFT, RIGHT), LIMIT(2), ONFAIL(TRANSFORM(rec, SELF.payload := '***FAILED***', SELF.key := RIGHT.key; SELF.aux := RIGHT.aux))))
);
