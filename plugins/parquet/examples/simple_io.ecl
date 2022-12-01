IMPORT Parquet;

simpleRec := RECORD
    UNSIGNED4 num;
		REAL balance;
		UTF8_de lastname;
    STRING name;
END; 
simpleDataset := DATASET([{1, 2.4356, U'de\3531', 'Jack'}, {1, 2.4356, U'de\3531', 'Jack'}, {2, 4.8937, U'as\352df', 'John'}, {3, 1.8573, 'nj\351vf', 'Jane'}, {4, 9.1235, U'ds\354fg', 'Jill'}, {5, 6.3297, U'po\355gm', 'Jim'}], simpleRec);

Write(simpleDataset, '/datadrive/dev/test_data/simple.parquet');

read_in := Read(simpleRec, '/datadrive/dev/test_data/simple.parquet');
OUTPUT(read_in, NAMED('SIMPLE_PARQUET_IO'));