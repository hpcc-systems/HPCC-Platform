/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

//class=textsearch

#option ('checkAsserts',false);
#onwarning (3164, ignore); // The merge of tokens has single entries from some of the dataset, and the merge criteria orders between the input datasets

import $.SetupText;
import $.Options;
import Std.File;

boolean createMultiPart := (__PLATFORM__[1..4] = 'thor');
Files := $.Files(createMultiPart, false);

string OriginalTextFilesIp := '.' : STORED('OriginalTextFilesIp');
string OriginalTextFilesPath := '' : STORED('OriginalTextFilesEclPath');

rebuildSearchIndex := OriginalTextFilesIp <> '' AND OriginalTextFilesPath <> '';

IF(rebuildSearchIndex,
    SetupText.createSearchIndex(),
    File.DeleteLogicalFile(Files.NameSearchIndex, true));
