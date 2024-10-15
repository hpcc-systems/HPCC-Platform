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
//fail

//This ECL code reads an empty Parquet file, handling the case where it might be empty
//by outputting either the file contents or a single informative record if the file is empty.

IMPORT Parquet, Std.Uni;

RECORDDEF:= RECORD
    UNSIGNED4 index;
    STRING name;
    STRING director;
END;

filePath1 := '/var/lib/HPCCSystems/mydropzone/empty.parquet';

EMPTY_PARQUET := ParquetIO.Read(RECORDDEF, filePath1);

EMPTY_RESULT := IF(COUNT(EMPTY_PARQUET) = 0,
                   DATASET([{0, 'Empty Parquet File', ''}], RECORDDEF),
                   EMPTY_PARQUET);

OUTPUT(EMPTY_RESULT);
