IMPORT Parquet;

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
nested_dataset := DATASET([{U'J\353ck', U'\353ackson', { {22, 2, ['James', 'Jonathon']}, 5.9, 600}}, {'John', 'Johnson', { {17, 0, []}, 6.3, 18}},
                                {'Amy', U'Amy\353on', { {59, 1, ['Andy']}, 3.9, 59}}, {'Grace', U'Graceso\353', { {11, 3, ['Grayson', 'Gina', 'George']}, 7.9, 100}}], parentRec);

writeStepA := ParquetIO.Write(nested_dataset, '/datadrive/dev/test_data/sandbox/nested_A.parquet', TRUE);
writeStepB := ParquetIO.Write(nested_dataset, '/datadrive/dev/test_data/sandbox/nested_B.parquet', TRUE);

read_in_a := ParquetIO.Read(parentRec, '/datadrive/dev/test_data/sandbox/nested_A.parquet');
readStepA := OUTPUT(read_in_a, NAMED('NESTED_PARQUET_IO_A'));
read_in_b := ParquetIO.Read(parentRec, '/datadrive/dev/test_data/sandbox/nested_B.parquet');
readStepB := OUTPUT(read_in_b, NAMED('NESTED_PARQUET_IO_B'));

SEQUENTIAL(writeStepA, writeStepB, readStepA, readStepB);
// SEQUENTIAL(readStepA, readStepB);