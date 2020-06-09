DistributeByValue(VIRTUAL DATASET inFile,
                         <?> ANY numericField,
                         UNSIGNED2 nodeCount = CLUSTERSIZE) := FUNCTION
    minMax := TABLE
        (
            inFile,
            {
                TYPEOF(inFile.<numericField>)  minValue := MIN(GROUP, <numericField>),
                TYPEOF(inFile.<numericField>)  maxValue := MAX(GROUP, <numericField>)
            }
        );
    spanValue := MAX(ROUNDUP((minMax[1].maxValue - minMax[1].minValue) / MAX(nodeCount, 1) + 1), 1);
    RETURN DISTRIBUTE(inFile, MAX(<numericField> - minMax[1].minValue, 1) DIV spanValue);
END;


ds := DATASET('test', { string name, unsigned value }, thor);
output(DistributeByValue(ds, value));
