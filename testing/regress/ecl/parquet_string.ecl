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

filePath := '/var/lib/HPCCSystems/mydropzone/string_dataset.parquet';

importedDataset := ParquetIO.Read(recordLayout, filePath);

writeStep := ParquetIO.Write(importedDataset, filePath, TRUE);

outputDataset := ParquetIO.Read(recordLayout, filePath);

compareDatasets := IF(COUNT(importedDataset) = COUNT(outputDataset),
                      'Pass',
                      'Fail: Data count mismatch');


SEQUENTIAL(
    writeStep,
    OUTPUT(compareDatasets, NAMED('StringDatasetTest'))
);

IF(compareDatasets = 'Fail: Data count mismatch',
   OUTPUT(outputDataset, NAMED('output_dataset'))
);


