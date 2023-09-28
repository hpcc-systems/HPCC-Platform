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

parquet_data := Read(layout, '/datadrive/dev/test_data/ghtorrent-2019-01-07.parquet');
part_a := CHOOSEN(parquet_data, 1000);
OUTPUT(CHOOSEN(DATASET('~parquet::large::ghtorrent-2019-02-04.csv', layout, CSV(HEADING(1))), 1000), NAMED('CSV_GHTORRENT'));
OUTPUT(part_a, NAMED('GHTORRENT'));

Write(part_a, '/datadrive/dev/test_data/github_partition/part_a.parquet');