//nothor
//noroxie
//version rowCount=25000
//version rowCount=50000
//version rowCount=100000

import ^ as root;
rowCount := #IFDEFINED(root.rowCount, 44000);

//--- end of version configuration ---

taxiRecord := RECORD
    STRING VendorID {XPATH('VendorID')};
    STRING tpep_pickup_datetime;
    STRING tpep_dropoff_datetime;
    STRING passenger_count;
    STRING trip_distance;
    STRING RatecodeID {XPATH('RatecodeID')};
    STRING store_and_fwd_flag;
    STRING PULocationID {XPATH('PULocationID')};
    STRING DOLocationID {XPATH('DOLocationID')};
    STRING payment_type;
    STRING fare_amount;
    STRING extra;
    STRING mta_tax;
    STRING tip_amount;
    STRING tolls_amount;
    STRING improvement_surcharge;
    STRING total_amount;
    STRING congestion_surcharge;
    STRING airport_fee;
END;

githubRecord := RECORD
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

taxiData := DATASET('~file::.::datadrive::dev::test_data::yellow_tripdata_2023-01.parquet', taxiRecord, TYPE(PARQUET: maxRowCountInTable(rowCount)));
OUTPUT(CHOOSEN(taxiData, 500), NAMED('TAXI_PARQUET'));
OUTPUT(COUNT(NOCOMBINE(taxiData)), NAMED('TAXI_PARQUET_COUNT'));

githubData := DATASET('~file::.::datadrive::dev::test_data::sandbox::github_data*.parquet', githubRecord, TYPE(PARQUET: maxRowCountInTable(rowCount)));
OUTPUT(CHOOSEN(githubData, 500), NAMED('GITHUB_PARQUET'));
OUTPUT(COUNT(NOCOMBINE(githubData)), NAMED('GITHUB_PARQUET_COUNT'));