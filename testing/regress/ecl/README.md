# Test Suite for the Parquet Plugin

## Running the Test Suite

The Parquet plugin test suite is a subset of tests in the HPCC Systems regression suite. To run the tests:

Change directory to HPCC Platform/testing/regress.

To run the entire Parquet test suite:

Note: Some Parquet tests require initialization of Parquet files. Use the ```./ecl-test setup```command to initialize these files before running the test suite.

These commands can be run on any cluster, including hthor or Roxie like the example below.

```
./ecl-test query --runclass parquet parquet*.ecl
```

To run a single test file:
```
./ecl-test query --runclass parquet <test_file_name>.ecl

example below:

/ecl-test query --target hthor --runclass parquet  parquet_schema.ecl

```

On the roxie cluster:
```
./ecl-test query --target roxie --runclass parquet parquet*.ecl
```
This is what you should see when you run the command above:
```
[Action] Suite: roxie
[Action] Queries: 15
[Action]   1. Test: parquet_compress.ecl ( version: compressionType='UNCOMPRESSED' )
[Pass]   1. Pass parquet_compress.ecl ( version: compressionType='UNCOMPRESSED' ) - W20240815-111429 (4 sec)
[Pass]   1. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111429
[Action]   2. Test: parquet_compress.ecl ( version: compressionType='Snappy' )
[Pass]   2. Pass parquet_compress.ecl ( version: compressionType='Snappy' ) - W20240815-111434 (3 sec)
[Pass]   2. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111434
[Action]   3. Test: parquet_compress.ecl ( version: compressionType='GZip' )
[Pass]   3. Pass parquet_compress.ecl ( version: compressionType='GZip' ) - W20240815-111438 (4 sec)
[Pass]   3. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111438
[Action]   4. Test: parquet_compress.ecl ( version: compressionType='Brotli' )
[Pass]   4. Pass parquet_compress.ecl ( version: compressionType='Brotli' ) - W20240815-111442 (4 sec)
[Pass]   4. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111442
[Action]   5. Test: parquet_compress.ecl ( version: compressionType='LZ4' )
[Pass]   5. Pass parquet_compress.ecl ( version: compressionType='LZ4' ) - W20240815-111447 (3 sec)
[Pass]   5. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111447
[Action]   6. Test: parquet_compress.ecl ( version: compressionType='ZSTD' )
[Pass]   6. Pass parquet_compress.ecl ( version: compressionType='ZSTD' ) - W20240815-111450 (2 sec)
[Pass]   6. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111450
[Action]   7. Test: parquet_corrupt.ecl
[Pass]   7. Pass parquet_corrupt.ecl - W20240815-111453 (2 sec)
[Pass]   7. Intentionally fails
[Pass]   7. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111453
[Action]   8. Test: parquet_empty.ecl
[Pass]   8. Pass parquet_empty.ecl - W20240815-111455 (2 sec)
[Pass]   8. Intentionally fails
[Pass]   8. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111455
[Action]   9. Test: parquet_overwrite.ecl
[Pass]   9. Pass parquet_overwrite.ecl - W20240815-111457 (2 sec)
[Pass]   9. Intentionally fails
[Pass]   9. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111457
[Action]  10. Test: parquet_partition.ecl
[Pass]  10. Pass parquet_partition.ecl - W20240815-111459 (2 sec)
[Pass]  10. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111459
[Action]  11. Test: parquet_schema.ecl
[Pass]  11. Pass parquet_schema.ecl - W20240815-111502 (1 sec)
[Pass]  11. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111502
[Action]  12. Test: parquet_size.ecl
[Pass]  12. Pass parquet_size.ecl - W20240815-111504 (3 sec)
[Pass]  12. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111504
[Action]  13. Test: parquet_string.ecl
[Pass]  13. Pass parquet_string.ecl - W20240815-111507 (1 sec)
[Pass]  13. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111507
[Action]  14. Test: parquet_types.ecl
[Pass]  14. Pass parquet_types.ecl - W20240815-111509 (7 sec)
[Pass]  14. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111509
[Action]  15. Test: parquet_write.ecl
[Pass]  15. Pass parquet_write.ecl - W20240815-111517 (2 sec)
[Pass]  15. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20240815-111517
[Action]
    -------------------------------------------------
    Result:
    Passing: 15
    Failure: 0
    -------------------------------------------------
    Log: /home/user/HPCCSystems-regression/log/roxie.24-08-15-11-14-29.log
    -------------------------------------------------
    Elapsed time: 52 sec  (00:00:52)
    -------------------------------------------------
```
## Project Description

This project focuses on the development of a comprehensive test suite for the recently integrated Parquet plugin within the HPCC Systems platform. The objective is to thoroughly evaluate the plugin's functionality, performance, and robustness across different scenarios and configurations. The key deliverables include defining and implementing various test cases, fixing any identified bugs, and providing extensive documentation.

The test suite will evaluate all data types supported by ECL and Arrow, as well as file operations, various compression formats, and schema handling. Additionally, the test suite will measure the plugin's performance across different HPCC components and hardware configurations, conduct stress tests to identify potential bottlenecks and bugs, and compare Parquet to other file formats used in the ecosystem, such as JSON, XML, and CSV.

# Test Files
The test suite consists of 10 main test files that test different parquet functionality and operations:

- parquet_types.ecl: Tests various ECL and Arrow data types

- parquet_schema.ecl: Evaluates Parquet's handling of different schemas

- parquet_compress.ecl: Tests different compression algorithms

- parquet_write.ecl: Validates Parquet write operations

- parquet_empty.ecl: Tests behavior with empty Parquet files

- parquet_corrupt.ecl: Checks handling of corrupt Parquet data

- parquet_size.ecl: Compares file sizes across formats

- parquet_partition.ecl: Tests partitioning in Parquet files

- parquet_overwrite.ecl: Validates overwrite operations

- parquet-string.ecl: Focuses on string-related operations


## Test Suite Overview
## Type Testing

Covers 42 data types including ECL and Arrow types
Examples: BOOLEAN, INTEGER, STRING, UNICODE, various numeric types, sets, and Arrow-specific types

 ### Data Type Tests

The Parquet plugin test suite shows that the plugin supports all ECL types.

#### Arrow Types Supported by the Parquet Plugin
- null
- uint8
- int8
- uint16
- int16
- uint32
- int32
- uint64
- int64
- half_float
- float
- double
- string
- binary
- fixed_size_binary
- date32
- date64
- timestamp
- time32
- time64
- interval_months
- list
- decimal
- large_list
- interval_day_time


# Compression Testing

Tests all available Arrow compression types: Snappy, GZip, Brotli, LZ4, ZSTD, Uncompressed
Compares performance and file sizes for different compression options

### Parquet Read and Write Operations

Tests ParquetIO.Read for creating ECL datasets from Parquet files
Tests ParquetIO.Write for writing ECL datasets to Parquet files

# Additional Tests

Read and Write Speeds Comparison with Other File Types
Schema Handling and Compatibility
Behavior with Corrupt Data and Empty Parquet Files

# Test Evaluation

The test suite generally uses key files located in HPCC-Platform/testing/regress/ecl/key, with a ".xml" extension, to evaluate test outcomes. These files store the expected results for comparison. However, some Parquet tests do not rely on key files, and alternative evaluation methods are used in those cases.
