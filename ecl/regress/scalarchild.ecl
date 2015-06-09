//fail

idRec := {unsigned id };

ds := DATASET(10, TRANSFORM(idRec, SELF.id := HASH64(COUNTER)));

f(unsigned id) := FUNCTION
    o := OUTPUT(id);
    RETURN WHEN(id * 2, o);
END;

idRec t(idRec l) := TRANSFORM
    SELF.id := f(l.id);
END;

p := PROJECT(NOFOLD(ds), t(LEFT));

output(p);
