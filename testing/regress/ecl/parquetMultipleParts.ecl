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

// Run regression suite setup to install the multiPart.parquet files. There are two files with clashing part masks.
// multiPart._1_of_2.parquet and multiPart.2_of_5.parquet
// The Read should fail because the plugin finds multiple partmasks of different sized datasets.

IMPORT Parquet;

recordLayout := RECORD
    UNSIGNED4 id;
    STRING name;
    REAL8 price;
    STRING isactive;
END;

string basePath := '' : STORED('OriginalTextFilesOsPath');

filePath := basePath + '/download/multiPart.parquet';

multiDataset := ParquetIO.Read(recordLayout, filePath);

OUTPUT(multiDataset, NAMED('multiDataset'))     // Output for the combined multi-part files
