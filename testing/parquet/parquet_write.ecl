//class=ParquetRegression

IMPORT Parquet;
IMPORT Std;

SimpleRecord := RECORD
    INTEGER id;
    STRING name;
    DECIMAL8 price;
    BOOLEAN isActive;
END;

STRING generateRandomName(INTEGER id) := 'Product ' + CHOOSE(id % 10 + 1, 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J');

smallDataset := DATASET(50, TRANSFORM(SimpleRecord,
    SELF.id := COUNTER,
    SELF.name := generateRandomName(COUNTER),
    SELF.price := RANDOM() * 100,
    SELF.isActive := RANDOM() % 2 = 0
));

mediumDataset := DATASET(250, TRANSFORM(SimpleRecord,
    SELF.id := COUNTER,
    SELF.name := generateRandomName(COUNTER),
    SELF.price := RANDOM() * 100,
    SELF.isActive := RANDOM() % 2 = 0
));

ParquetIO.Write(smallDataset, '/var/lib/HPCCSystems/mydropzone/small1.parquet', TRUE);
ParquetIO.Write(mediumDataset, '/var/lib/HPCCSystems/mydropzone/medium1.parquet', TRUE);

smallReadbackData := ParquetIO.Read(SimpleRecord, '/var/lib/HPCCSystems/mydropzone/small1.parquet');
mediumReadbackData := ParquetIO.Read(SimpleRecord, '/var/lib/HPCCSystems/mydropzone/medium1.parquet');

OUTPUT(smallReadbackData, NAMED('SmallDataset'));
OUTPUT(mediumReadbackData, NAMED('MediumDataset'));

