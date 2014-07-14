SubRec := RECORD
    UNSIGNED            s;
END;

DataRec := RECORD
    UNSIGNED            i;
    DATASET(SubRec)     d;
END;

r := DATASET
    (
        [
            {1,DATASET([100,200],SubRec)},
            {2,DATASET([300,400],SubRec)}
        ],
        DataRec
    );

r;
r.d;
