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

#IF(0)
csv_data := DATASET('~parquet::large::ghtorrent-2019-02-04.csv', layout, CSV(HEADING(1)));
Write(csv_data, '/datadrive/dev/test_data/ghtorrent-2019-02-04.parquet');
#END

#IF(1)
parquet_data := Read(layout, '/datadrive/dev/test_data/hpcc_gh_partition/data.parquet');
OUTPUT(COUNT(parquet_data), NAMED('ghtorrent_2019_01_07'));
#END