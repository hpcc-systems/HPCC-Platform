Layout := RECORD
    DECIMAL18_8     amount;
    UNSIGNED8       precision;
END;

testData0 := DATASET
    (
        [
            {2.3445, 2},
            {2.3445, 3}
        ],
        Layout
    );

testData := NOFOLD(testData0);

roundedData := PROJECT
    (
        testData,
        TRANSFORM
            (
                {
                    RECORDOF(LEFT),
                    DECIMAL18_8     rounded
                },
                SELF.rounded := ROUND(LEFT.amount, LEFT.precision),
                SELF := LEFT
            )
    );

OUTPUT(roundedData, ALL);
