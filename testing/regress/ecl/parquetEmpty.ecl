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
//nothor
//noroxie

// This test tries writing an empty dataset to a Parquet file and then tries to read it.
// When writing Parquet files the plugin waits for a row before opening a file. The record
// is empty, therefore the file is never created and the read fails.

IMPORT Std;
IMPORT Parquet;

RECORDDEF := RECORD
    UNSIGNED4 index;
    STRING director;
END;

basePath := Std.File.GetDefaultDropZone() + '/regress/parquet/';
filePath := basePath + 'empty_test.parquet';

EMPTY_PARQUET := DATASET([], RECORDDEF);

ParquetIO.Write(EMPTY_PARQUET, filePath, TRUE);

read_data := ParquetIO.Read(RECORDDEF, filePath);
