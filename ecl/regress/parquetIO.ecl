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
writeStep := ParquetIO.Write(DISTRIBUTE(CHOOSEN(csv_data, 100000), SKEW(0.1)), '/datadrive/dev/test_data/sandbox/github_data.parquet', true);

parquet_data := ParquetIO.Read(layout, '/datadrive/dev/test_data/sandbox/github_data.parquet');
readStep := OUTPUT(CHOOSEN(parquet_data, 100), NAMED('ghtorrent_2019_01_07'));
countStep := COUNT(parquet_data);
SEQUENTIAL(writeStep, PARALLEL(readStep, countStep));