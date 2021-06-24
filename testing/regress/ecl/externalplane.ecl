/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

//nothor            HPCC-26144 plane:: currently not supported on thor

import Std.File;

rec := RECORD
 string f;
END;
ds := DATASET([{'a'}], rec);

dropzone := 'mydropzone' : STORED('dropzone');

string getFName(string ext) := FUNCTION
 RETURN '~PLANE::' + dropzone + '::' + 'external' + ext;
END;

external1 := getFName('1');
external2 := getFName('2');

SEQUENTIAL(
File.DeleteLogicalFile(external1, true),
File.DeleteLogicalFile(external2, true),
OUTPUT(ds,,external1),
OUTPUT(DATASET(external1, rec, FLAT), , external2),
OUTPUT(DATASET(external2, rec, FLAT), , external1, OVERWRITE),
OUTPUT(DATASET(external1, rec, FLAT)),
File.DeleteLogicalFile(external1), // NB: will fail if doesn't exist
File.DeleteLogicalFile(external2) // NB: will fail if doesn't exist
);
