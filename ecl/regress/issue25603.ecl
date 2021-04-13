DataRec := RECORD
    UNSIGNED4   n1;
    UNSIGNED4   n2 := 0;
END;

ds1 := DATASET
    (
        10,
        TRANSFORM
            (
                DataRec,
                SELF.n1 := RANDOM()
            )
    );

DataRec XForm(DataRec l) := TRANSFORM
    SELF := l;
END;

ds2 := PROJECT(NOFOLD(ds1), XForm(DataRec)); // Call transform with a RECORD?
ds2;
