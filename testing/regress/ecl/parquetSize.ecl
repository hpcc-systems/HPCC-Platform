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
##############################################################################*/

//class=parquet

IMPORT Parquet;

recordLayout := RECORD
    UNSIGNED4 id;
    STRING name;
    REAL8 price;
    STRING isactive;
END;

// Paths to the files
singleFilePath := '/var/lib/HPCCSystems/mydropzone/single.parquet';
multiFilePath := '/var/lib/HPCCSystems/mydropzone/multi.parquet';

// Reading the single and multi-part files
singleDataset := ParquetIO.Read(recordLayout, singleFilePath);
multiDataset := ParquetIO.Read(recordLayout, multiFilePath);

// Output the datasets
SEQUENTIAL(
    OUTPUT(singleDataset, NAMED('singleDataset')),  // Output for the single file
    OUTPUT(multiDataset, NAMED('multiDataset'))     // Output for the combined multi-part files
);