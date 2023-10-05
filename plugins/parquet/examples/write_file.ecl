IMPORT Parquet;

ds := DATASET
    (
        1000,
        TRANSFORM
            (
                {INTEGER n},
                SELF.n := RANDOM()
            ),
        DISTRIBUTED
    );

ParquetIO.Write(ds, '/var/lib/HPCCSystems/mydropzone/sample.parquet');
