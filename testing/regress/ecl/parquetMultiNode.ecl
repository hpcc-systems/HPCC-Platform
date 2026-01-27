/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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
//fail

// Attempt to read a file with missing parts. This typically occurs when reading files from
// a multi-node write where some parts are missing on certain nodes. In this test, the other files
// are not found by any of the threads. This typically represents a user error where files are
// located on different nodes.

IMPORT Parquet;

recordLayout := RECORD
    UNSIGNED4 id;
    STRING name;
    REAL8 price;
    STRING isactive;
END;

string basePath := '' : STORED('OriginalTextFilesOsPath');

filePath := basePath + '/download/multiNode.parquet';

OUTPUT(ParquetIO.Read(recordLayout, filePath), NAMED('READ_INCOMPLETE_PARQUET'));
