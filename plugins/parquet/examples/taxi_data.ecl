IMPORT PARQUET;

EXPORT Layout := RECORD
    STRING VendorID;
    STRING tpep_pickup_datetime;
    STRING tpep_dropoff_datetime;
    STRING passenger_count;
    STRING trip_distance;
    STRING RatecodeID;
    STRING store_and_fwd_flag;
    STRING PULocationID;
    STRING DOLocationID;
    STRING payment_type;
    STRING fare_amount;
    STRING extra;
    STRING mta_tax;
    STRING tip_amount;
    STRING tolls_amount;
    STRING improvement_surcharge;
    STRING total_amount;
END;

tripData := '/datadrive/dev/test_data/yellow_tripdata_2017-01.parquet';
read_in := ParquetIO.Read(Layout, tripData);
COUNT(read_in);
OUTPUT(CHOOSEN(read_in, 100));