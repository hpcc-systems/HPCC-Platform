/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//class=parquet

IMPORT Std;
IMPORT Parquet;

// Define the record layout with explicit field lengths
datasetRecordLayout := RECORD
    UNSIGNED4 district;
    STRING firstname;
    STRING lastname;
    UNSIGNED4 age;
    STRING city;
END;

// Create a small dataset - ensure all records have valid data
smallData := DATASET([
    {1, 'Alice', 'A', 30, 'New York'},
    {1, 'Alice', 'B', 35, 'New York'},
    {1, 'Chalice', 'C', 40, 'Boston'},
    {2, 'Bob', 'A', 25, 'Los Angeles'},
    {2, 'Jim', 'A', 25, 'Los Angeles'},
    {3, 'Charlie', 'C', 40, 'Chicago'}
], datasetRecordLayout);

// Set options
overwriteOption := TRUE;
rowSize := 1024;  // Increased buffer size

// Define base path (without 'file://' prefix)
basePath := Std.File.GetDefaultDropZone() + '/regress/parquet/';

// Define partition keys as a semicolon-separated string with all keys
partitionKeys := 'district;firstname;city';

/**
 * This partitioning creates a structure like this:
 *
 * Hive Partitioning:
 * ├── district=1
 * │   ├── firstname=Alice
 * │   │   └── city=New%20York
 * │   │       └── part_0_of_table_0_from_worker_0.parquet
 * │   └── firstname=Chalice
 * │       └── city=Boston
 * │           └── part_0_of_table_0_from_worker_0.parquet
 * ├── district=2
 * │   ├── firstname=Bob
 * │   │   └── city=Los%20Angeles
 * │   │       └── part_0_of_table_0_from_worker_0.parquet
 * │   └── firstname=Jim
 * │       └── city=Los%20Angeles
 * │           └── part_0_of_table_0_from_worker_0.parquet
 * └── district=3
 *     └── firstname=Charlie
 *         └── city=Chicago
 *             └── part_0_of_table_0_from_worker_0.parquet
 *
 * Directory Partitioning:
 * ├── 1
 * │   ├── Alice
 * │   │   └── New York
 * │   │       └── part_0_of_table_0_from_worker_0.parquet
 * │   └── Chalice
 * │       └── Boston
 * │           └── part_0_of_table_0_from_worker_0.parquet
 * ├── 2
 * │   ├── Bob
 * │   │   └── Los Angeles
 * │   │       └── part_0_of_table_0_from_worker_0.parquet
 * │   └── Jim
 * │       └── Los Angeles
 * │           └── part_0_of_table_0_from_worker_0.parquet
 * └── 3
 *     └── Charlie
 *         └── Chicago
 *             └── part_0_of_table_0_from_worker_0.parquet
 */

// Write out the dataset with Hive partitioning on all keys
ParquetIO.HivePartition.Write(
    smallData,
    rowSize,
    basePath + 'hive_partitioned/',
    overwriteOption,
    partitionKeys
);

// Write out the dataset with Directory partitioning on all keys
ParquetIO.DirectoryPartition.Write(
    smallData,
    rowSize,
    basePath + 'dir_partitioned/',
    overwriteOption,
    partitionKeys
);

// Define file paths for partitioned datasets
hiveFilePath := basePath + 'hive_partitioned/';
dirFilePath := basePath + 'dir_partitioned/';

// Read back the partitioned data
readBackHiveData := ParquetIO.HivePartition.Read(datasetRecordLayout, hiveFilePath);
readBackDirData := ParquetIO.DirectoryPartition.Read(datasetRecordLayout, dirFilePath, partitionKeys);

// Output the entire dataset for verification
OUTPUT(readBackHiveData, NAMED('HivePartitionedSampleData'));
OUTPUT(readBackDirData, NAMED('DirPartitionedSampleData'));

