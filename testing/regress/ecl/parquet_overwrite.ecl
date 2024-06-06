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
//fail

IMPORT Parquet;

SingleRowDataset := DATASET([{1, 'SingleRow', TRUE}], {UNSIGNED id, STRING name, BOOLEAN flag});

ParquetIO.write(SingleRowDataset, '/var/lib/HPCCSystems/mydropzone/SingleRowTest.parquet');

ConflictingDataset := DATASET([{2, 'OverwrittenRow', 123}], {UNSIGNED id, STRING name, INTEGER conflict});  // Schema conflict

ParquetIO.write(ConflictingDataset, '/var/lib/HPCCSystems/mydropzone/SingleRowTest.parquet');  // Assuming TRUE should force overwrite if the parameter is valid

