IMPORT STD;
IMPORT Parquet;

#OPTION('outputLimit', 2000);
#OPTION('pickBestEngine', FALSE);

layout := RECORD
    STRING actor_login;
    INTEGER actor_id;
    INTEGER comment_id;
    STRING comment;
    STRING repo;
    STRING language;
    STRING author_login;
    INTEGER author_id;
    INTEGER pr_id;
    INTEGER c_id;
    INTEGER commit_date;
END;

csv_data := DATASET('~parquet::large::ghtorrent-2019-02-04.csv', layout, CSV(HEADING(1)));
writeStep := ParquetIO.HivePartition.Write(CHOOSEN(csv_data, 1330), , '/datadrive/dev/test_data/sandbox/test_partition/', TRUE, 'commit_date;repo');

github_dataset := ParquetIO.HivePartition.Read(layout, '/datadrive/dev/test_data/sandbox/test_partition/');
readStep := OUTPUT(CHOOSEN(github_dataset, 100), NAMED('GITHUB_PARTITION'));
countStep := OUTPUT(COUNT(github_dataset), NAMED('GITHUB_COUNT'));

SEQUENTIAL(writeStep, PARALLEL(readStep, countStep));
