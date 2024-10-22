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

// Define the record layout for the dataset
datasetRecordLayout := RECORD
    INTEGER id;
    STRING name;
    INTEGER age;
    STRING city;
END;

// Create a small dataset
smallData := DATASET([
    {1, 'Alice', 30, 'New York'},
    {2, 'Bob', 25, 'Los Angeles'},
    {3, 'Charlie', 40, 'Chicago'}
], datasetRecordLayout);

// Set options
overwriteOption := TRUE;
rowSize := 1;

// Define base path using GetDefaultDropZone()
basePath := Std.File.GetDefaultDropZone() + 'regress/parquet/';

// Write out the dataset with Hive partitioning on CITY
ParquetIO.HivePartition.Write(
    smallData,
    rowSize,                            // Number of rows per file
    basePath + 'hive_partitioned/',
    overwriteOption,                    // Overwrite existing files
    'city'                              // Partition key
);

// Write out the dataset with Directory partitioning on AGE
ParquetIO.DirectoryPartition.Write(
    smallData,                          // Data to write
    rowSize,                            // Number of rows per file
    basePath + 'dir_partitioned/',
    overwriteOption,                    // Overwrite existing files
    'age'                               // Partition key
);

// Define file paths for partitioned datasets
hiveFilePath := basePath + 'hive_partitioned/';
dirFilePath := basePath + 'dir_partitioned/';

// Read back the partitioned data
readBackHiveData := ParquetIO.HivePartition.Read(datasetRecordLayout, hiveFilePath);
readBackDirData := ParquetIO.DirectoryPartition.Read(datasetRecordLayout, dirFilePath, 'age');

// Output the entire dataset for verification
OUTPUT(readBackHiveData, NAMED('HivePartitionedSampleData'));
OUTPUT(readBackDirData, NAMED('DirPartitionedSampleData'));