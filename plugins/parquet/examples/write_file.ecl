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

Parquet.Write(ds, '/var/lib/HPCCSystems/mydropzone/sample.parquet');
