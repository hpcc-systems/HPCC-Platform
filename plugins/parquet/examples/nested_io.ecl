IMPORT Parquet;

friendsRec :=RECORD
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

#IF(1)
ParquetIO.Write(nested_dataset, '/datadrive/dev/test_data/nested.parquet');
#END

#IF(1)
read_in := ParquetIO.Read(parentRec, '/datadrive/dev/test_data/nested.parquet');
OUTPUT(read_in, NAMED('NESTED_PARQUET_IO'));
#END