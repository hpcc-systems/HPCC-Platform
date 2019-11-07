#onwarning (1021, ignore); // Ignore warning that all output values are constant

r1 := RECORD
   string35 code;
  END;

r2 := RECORD
  integer2 id;
  embedded DATASET(r1) child;
 END;

mk1(unsigned c) := TRANSFORM(r1, SELF.code := (string)c; SELF := []);
mk2(unsigned c) := TRANSFORM(r2, SELF.id := c; SELF.child := DATASET((c-1) % 5, mk1(COUNTER+c)), SELF := []);

d := DATASET(20, mk2(counter));
sequential(
    output(d,,'~myspill', overwrite);
    output(DATASET('~myspill', r2, THOR), { one := 1 })
);
