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
github_dataset := Read(layout, '/datadrive/dev/test_data/ghtorrent-2019-01-07.parquet');
Write(DISTRIBUTE(github_dataset, SKEW(.05)), '/datadrive/dev/test_data/hpcc_gh_partition/data.parquet');
#END

#IF(1)
github_dataset := ReadPartition(layout, '/datadrive/dev/test_data/hpcc_gh_partition');
OUTPUT(COUNT(github_dataset), NAMED('GITHUB_PARTITION'));
#END