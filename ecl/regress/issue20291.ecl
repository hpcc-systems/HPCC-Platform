ds := DATASET([1,2,3,4,5], {UNSIGNED1 n});

res := PROJECT
    (
        NOFOLD(ds[1]),
        TRANSFORM
            (
                {STRING s},
                SELF.s := IF(LEFT.n % 2 = 0, SKIP, 'x') + '-' + IF(LEFT.n % 2 = 1, SKIP, 'y')
            )
    );

OUTPUT(res);
