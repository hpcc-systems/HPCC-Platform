#option ('targetClusterType', 'roxie');

inrec := RECORD
  UNSIGNED6 did;
END;
outrec := RECORD(inrec)
  STRING20  name;
  STRING10  ssn;
  UNSIGNED8  dob;
END;
ds := DATASET([1,2,3,4,5,6], inrec);
i1 := DATASET([{1, 'Kevin'}, {2, 'Richard'}, {5,'Nigel'}],
    { UNSIGNED6 did, STRING10 name });
i2 := DATASET([{3, '123462'}, {5, '1287234'}, {6,'007001002'}],
    { UNSIGNED6 did, STRING10 ssn });
i3 := DATASET([{1, 19700117}, {4, 19831212}, {6,20000101}],
    { UNSIGNED6 did, UNSIGNED8 dob}); 
j1 := JOIN(ds, i1, LEFT.did = RIGHT.did, LEFT OUTER, LOOKUP);
j2 := JOIN(ds, i2, LEFT.did = RIGHT.did, LEFT OUTER, LOOKUP);
j3 := JOIN(ds, i3, LEFT.did = RIGHT.did, LEFT OUTER, LOOKUP);
combined1 := COMBINE(j1, j2, TRANSFORM(outRec, SELF := LEFT; SELF := RIGHT; SELF := []));
// combined2 := COMBINE(combined1, j3, TRANSFORM(outRec, SELF.dob := RIGHT.dob; SELF := LEFT));
combined2 := COMBINE(combined1, j3);

output(combined2);
