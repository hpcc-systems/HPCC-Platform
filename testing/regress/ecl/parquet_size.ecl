/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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
//nothor,noroxie


IMPORT Parquet;

recordLayout := RECORD
    UNSIGNED4 index;
    STRING name;
    STRING director;
END;

smallFilePath := '/var/lib/HPCCSystems/mydropzone/small_dataset.parquet';
mediumFilePath := '/var/lib/HPCCSystems/mydropzone/medium_dataset.parquet';
largeFilePath := '/var/lib/HPCCSystems/mydropzone/large_dataset.parquet';
largestFilePath := '/var/lib/HPCCSystems/mydropzone/largest_dataset.parquet';

smallDataset := ParquetIO.Read(recordLayout, smallFilePath);
mediumDataset := ParquetIO.Read(recordLayout, mediumFilePath);
largeDataset := ParquetIO.Read(recordLayout, largeFilePath);
largestDataset := ParquetIO.Read(recordLayout, largestFilePath);

SEQUENTIAL(
    OUTPUT(smallDataset, NAMED('small_dataset')),
    OUTPUT(mediumDataset, NAMED('medium_dataset')),
    OUTPUT(largeDataset, NAMED('large_dataset')),
    OUTPUT(largestDataset, NAMED('largest_dataset'))
);

