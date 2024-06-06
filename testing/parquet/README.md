# Development of Test Suite for HPCC Systems Parquet Plugin

## Project Description

# Development of Test Suite for HPCC Systems Parquet Plugin

## Project Description

This project focuses on the development of a comprehensive test suite for the recently integrated Parquet plugin within the HPCC Systems platform. The objective is to thoroughly evaluate the plugin's functionality, performance, and robustness across different scenarios and configurations. The key deliverables include defining and implementing various test cases, fixing any identified bugs, and providing extensive documentation.

The test suite will evaluate all data types supported by ECL and Arrow, as well as file operations, various compression formats, and schema handling. Additionally, the test suite will measure the plugin's performance across different HPCC components and hardware configurations, conduct stress tests to identify potential bottlenecks and bugs, and compare Parquet to other file formats used in the ecosystem, such as JSON, XML, and CSV.

## Specific Information

### Test Suite Overview

- Data types testing(tested 42 data type's)

- Compression testing

All available Arrow compression types: Snappy, GZip, Brotli, LZ4, ZSTD, Uncompressed
Performance and file size comparisons for different compression options

- Parquet read and write operations

Testing the Read function to create ECL datasets from Parquet files. ParquetIO.Read
Testing the Write function to write ECL datasets to Parquet files. ParquetIO.Write

- Spray and despray speeds of different file types against Parquet

- Parquet’s reaction with different schemas

- Testing Parquet with corrupt data  & an empty parquet file

### Test Suite Structure

The test suite for the HPCC Systems Parquet plugin will be built by defining functions in ECL that cover various aspects of Parquet file handling:

 **Data Type Tests**
Tested data types include:
ECL Types
- BOOLEAN: True/False values.
- INTEGER: Signed integer values.
- UNSIGNED: Unsigned integer values.
- REAL: Floating-point values.
- DECIMAL: Decimal values with specified precision.
- STRING: Fixed-length strings.
- DATA: Binary data converted to STRING.
- VARSTRING: Variable-length strings.
- QSTRING: Quick strings.
- UTF8: UTF-8 encoded strings.
- UNICODE: Unicode strings.
- SET OF INTEGER: Sets of integer values.
- VARUNICODE: Variable-length Unicode strings.
- REAL8 (FLOAT8): 8-byte floating-point values.
- SET OF STRING: Sets of string values.
- SET OF UNICODE: Sets of Unicode values.
- INTEGER1 (BYTE): 1-byte integer values.

Arrow Types Supported by the Parquet Plugin
- null
- uint8
- int8
- uint16
- int16
- uint32
- int32
- uint64
- int64
- half
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




