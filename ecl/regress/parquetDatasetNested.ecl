#OPTION('genericDiskReads', true);

friendsRec := RECORD
    UNSIGNED4 age;
    INTEGER2 friends;
    SET OF STRING friendsList;
END;

childRec := RECORD
     friendsRec friends;
     REAL height;
     REAL weight;
END;

parentRec := RECORD
    UTF8_de firstname;
	UTF8_de lastname;
    childRec details;
END;

read_in_a := DATASET('~file::.::datadrive::dev::test_data::sandbox::nested_^A.parquet', parentRec, TYPE(PARQUET: maxRowCountInTable(48800)));
readStepA := OUTPUT(read_in_a, NAMED('NESTED_PARQUET_IO_A'));
read_in_b := DATASET('~file::.::datadrive::dev::test_data::sandbox::nested_^B.parquet', parentRec, TYPE(PARQUET: maxRowCountInTable(48800)));
readStepB := OUTPUT(read_in_b, NAMED('NESTED_PARQUET_IO_B'));

SEQUENTIAL(readStepA, readStepB);