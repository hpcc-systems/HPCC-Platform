# Parquet Plugin for HPCC Systems

The Parquet Plugin for HPCC Systems is a powerful tool designed to facilitate the fast transfer of data stored in a columnar format to the ECL (Enterprise Control Language) data format. This plugin provides seamless integration between Parquet files and HPCC Systems, enabling efficient data processing and analysis.

## Installation

The Parquet Plugin comes bundled with the HPCC Platform, so there's no need for a separate download. When you install or update HPCC Systems, you'll automatically have access to the latest version of the Parquet Plugin.

If the Parquet plugin is missing, you can either ensure it's enabled by turning on the `-DUSE_PARQUET=ON` option during the build, or follow the instructions below to install it manually.

The plugin uses vcpkg and can be installed by creating a separate build directory from the platform and running the following commands:

```
cd ./parquet-build
cmake -DPARQUETEMBED=ON ../HPCC-Platform
make -j4 package
sudo dpkg -i ./hpccsystems-plugin-parquetembed_<version>.deb
```

## Documentation

[Doxygen](https://www.doxygen.nl/index.html) can be used to create nice HTML documentation for the code. Call/caller graphs are also generated for functions if you have [dot](https://www.graphviz.org/download/) installed and available on your path.

Assuming `doxygen` is on your path, you can build the documentation via:
```
cd plugins/parquet
doxygen Doxyfile
```

## Features

The Parquet Plugin offers the following main functions:

### Regular Files

#### 1. Reading Parquet Files

The Read function allows ECL programmers to create an ECL dataset from both regular and partitioned Parquet files. It leverages the Apache Arrow interface for Parquet to efficiently stream data from ECL to the plugin, ensuring optimized data transfer.

In order to read a Parquet file it is necessary to define the record structure of the file you intend to read with the names of the fields as stored in the Parquet file and the type that you wish to read them as. It is possible for a Parquet file to have field names that contain characters that are incompatible with the ECL field name definition. For example, ECL field names are case insensitive causing an issue when trying to read Parquet fields with uppercase letters. To read field names of this type an XPATH can be passed as seen in the following example:

```
layout := RECORD
    STRING name;
    STRING job_id {XPATH('jobID')};
END;

dataset := ParquetIO.Read(layout, '/source/directory/data.parquet');
```

#### 2. Writing Parquet Files

The Write function empowers ECL programmers to write ECL datasets to Parquet files. By leveraging the Parquet format's columnar storage capabilities, this function provides efficient compression and optimized storage for data. There is an optional argument that sets the overwrite behavior of the plugin. The default value is false meaning it will throw an error if the target file already exists. If overwrite is set to true the plugin will check for files that match the target path passed in and delete them first before writing new files.

The Parquet Plugin supports all available Arrow compression types. Specifying the compression when writing is optional and defaults to Uncompressed. The options for compressing your files are Snappy, GZip, Brotli, LZ4, ZSTD, Uncompressed.

```
overwriteOption := TRUE;
compressionOption := 'Snappy';

ParquetIO.Write(inDataset, '/output/directory/data.parquet', overwriteOption, compressionOption);
```

### Partitioned Files (Tabular Datasets)

The Parquet plugin supports both Hive Partitioning and Directory Partitioning. Hive partitioning uses a key-value partitioning scheme for selecting directory names. For example, the file under `dataset/year=2017/month=01/data0.parquet` contains only data for which the year equals 2017 and the month equals 01. The second partitioning scheme, Directory Partitioning, is similar, but rather than having key-value pairs the partition keys are inferred in the file path. For example, instead of having `/year=2017/month=01/day=01` the file path would be `/2017/01/01`.

#### 1. Reading Partitioned Files

For reading partitioned files, pass in the target directory to the read function of the type of partition you are using. For directory partitioning, a list of the field names that make up the partitioning schema is required because it is not included in the directory structure like hive partitioning.

```
github_dataset := ParquetIO.HivePartition.Read(layout, '/source/directory/partitioned_dataset');

github_dataset := ParquetIO.DirectoryPartition.Read(layout, 'source/directory/partitioned_dataset', 'year;month;day')
```

#### 2. Writing Partitioned Files

To select the fields that you wish to partition your data on pass in a string of semicolon seperated field names. If the fields you select create too many subdirectories you may need to partition your data on different fields. The rowSize field defaults to 100000 rows and determines how many rows to put in each part of the output files. Writing a partitioned file to a directory that already contains data will fail unless the overwrite option is set to true. If the overwrite option is set to true and the target directory is not empty the plugin will first erase the contents of the target directory before writing the new files.

```
ParquetIO.HivePartition.Write(outDataset, rowSize, '/source/directory/partioned_dataset', overwriteOption, 'year;month;day');

ParquetIO.DirectoryPartition.Write(outDataset, rowSize, '/source/directory/partioned_dataset', overwriteOption, 'year;month;day');
```

## Apache Parquet/Arrow to ECL Type Mappings
### ECL Record to Arrow Schema Mappings

| ECL Record Type | Apache Arrow/Parquet Type | Notes |
|:----------------|:--------------------------|:------|
| BOOLEAN         | Boolean                   |       |
| INTEGER         | Int64                     | Defaults to 8 bytes (64-bit). Can be explicitly sized using INTEGER1, INTEGER2, INTEGER4, or INTEGER8 |
| INTEGER8        | Int64                     |       |
| UNSIGNED        | UInt64                    | Defaults to 8 bytes (64-bit). Can be explicitly sized using UNSIGNED1, UNSIGNED2, UNSIGNED4, or UNSIGNED8 |
| UNSIGNED8       | UInt64                    |       |
| REAL4           | Float                     |       |
| REAL8           | Double                    |       |
| DECIMAL         | LargeString               |       |
| STRING          | LargeString               |       |
| VARSTRING       | LargeString               |       |
| UTF8            | LargeString               |       |
| DATA            | LargeBinary               |       |
| DATA[n]         | FixedSizeBinary[n]        | Where n is the fixed length |
| SET OF          | LargeList                 |       |
| RECORD          | Struct                    |       |


### Default Supported Parquet/Arrow Types for ECL

| Apache Parquet / Arrow Type   | Notes |
|:------------------------------|:------|
| Boolean                       |       |
| Int8, Int16, Int32, Int64     | Size in ECL depends on the Arrow type |
| UInt8, UInt16, UInt32, UInt64 | Size in ECL depends on the Arrow type |
| Float16 (Half Float)          | Stored as REAL4 or REAL8. Processed in 64-bit buffer internally  |
| Float                         |       |
| Double                        |       |
| Decimal128                    | Precision and scale in the Parquet file schema should match the ECL record definition|
| Decimal256                    | Precision and scale in the Parquet schema should match the ECL record definition. Note that arrow::Decimal256 supports larger precision and scale than the 64-digit maximum in ECL. |
| Date32, Date64                | Stored as days since epoch |
| Time32, Time64                | Stored as milliseconds or microseconds since midnight (time of day) |
| Timestamp                     | Stored as microseconds since epoch |
| Duration                      | Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds |
| LargeString                   |       |
| LargeBinary                   |       |
| FixedSizeBinary               | Fixed-length binary data |
| LargeList                     |       |
| FixedSizeList                 | With fixed number of elements |
| Struct                        | Nested structure |

For more detailed information about Apache Arrow data types, refer to the [Apache Arrow Documentation](https://arrow.apache.org/docs/cpp/api/datatype.html).

## Example Usage


```
IMPORT Parquet;

// Record definition with both fixed-size and large binary fields
BinaryTypesRecord := RECORD
    UNSIGNED1 id;
    DATA10 fixedBinary;  // Fixed-size binary (10 bytes)
    DATA largeBinary;    // Large binary (variable size)
END;

// Sample dataset
sampleData := DATASET([
    {1, (DATA10)123.45, (DATA)6789.01},
    {2, (DATA10)234.56, (DATA)7890.12}
], BinaryTypesRecord);

// Write and read using ParquetIO
ParquetIO.Write(sampleData, '/var/lib/HPCCSystems/mydropzone/BinaryTypes.parquet');
readData := ParquetIO.Read(BinaryTypesRecord, '/var/lib/HPCCSystems/mydropzone/BinaryTypes.parquet');

OUTPUT(readData);
```
This example demonstrates how to define an ECL record structure with fixed-size and variable-size binary fields, create a sample dataset, write it to a Parquet file, and then read it back using ParquetIO, showcasing the handling of different binary data types in HPCC Systems.
