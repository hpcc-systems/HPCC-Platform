Dbg(sym) := MACRO
    OUTPUT(sym, NAMED(#TEXT(sym)));
ENDMACRO;

DataRec := RECORD
    UNSIGNED4   n;
    UNSIGNED4   i;
END;

ds1 := DATASET
    (
        5,
        TRANSFORM
            (
                DataRec,
                SELF.n := RANDOM(),
                SELF.i := RANDOM()
            )
    );

ds2 := DATASET(['foo', 'bar'], {STRING s});

Dbg(ds1);
Dbg(ds2);

ds3 := DEDUP(ds1, ds2, WHOLE RECORD, ALL);
Dbg(ds3);

ds4 := SORT(ds1, ds1, n);
Dbg(ds4);
