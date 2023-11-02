IMPORT PARQUET;

EXPORT Layout := RECORD
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

tripData := '/datadrive/dev/test_data/yellow_tripdata_2023-01.parquet';
read_in := ParquetIO.Read(Layout, tripData);
OUTPUT(CHOOSEN(read_in, 100));