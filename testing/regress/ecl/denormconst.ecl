ChildRec := RECORD
    INTEGER1            nameID;
    STRING20            addr;
END;

ParentRec := RECORD
    INTEGER1            nameID;
    STRING20            name;
    BOOLEAN             hasChildren := FALSE;
    INTEGER1            numRows := 0;
    DATASET(ChildRec)   children := DATASET([], ChildRec);
END;

namesTable := DATASET
    (
        [
            {1, 'Gavin'},
            {2, 'Liz'},
            {3, 'Mr Nobody'},
            {4, 'Anywhere'}
        ],
        ParentRec
    );

normAddrs := DATASET
    (
        [
            {1, '10 Malt Lane'},
            {2, '10 Malt Lane'},
            {2, '3 The cottages'}
        ],
        ChildRec
    );

res := DENORMALIZE
    (
        namesTable,
        normAddrs,
        LEFT.nameID = RIGHT.nameID,
        TRANSFORM
            (
                ParentRec,
                SELF.numRows := COUNTER,
                SELF.children := LEFT.children + RIGHT,
                SELF.hasChildren := TRUE,
                SELF := LEFT
            )
    );

OUTPUT(res, NAMED('AllResults'));
OUTPUT(res(hasChildren), NAMED('HasChildren'));
OUTPUT(res(~hasChildren), NAMED('DoesNotHaveChildren'));
