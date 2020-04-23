/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

//NB: testing superfile logic/dfs here, no need to run in all engines
//nothor
//noroxie

#onwarning (5401, ignore); // suppress info re. optional missing file
#onwarning (0, ignore); // suppress info re. DeleteLogicalFile

import Std.File AS FileServices;
import $.setup;

prefix := setup.Files(false, false).QueryFilePrefix;

rec := RECORD
 integer i;
 string1 id;
END;

ds1 := DATASET([{1,'A'}, {1,'B'}, {1,'C'}], rec);
ds2 := DATASET([{2,'D'}, {2,'E'}], rec);

inlinesuper := '{'+prefix+'subfile1,'+prefix+'subfile2}';
isuper := DATASET(inlinesuper, rec, FLAT);
isupero := DATASET(inlinesuper, rec, FLAT, OPT);

SEQUENTIAL(
  OUTPUT(ds1,,prefix + 'subfile1', OVERWRITE),
  OUTPUT(ds2,,prefix + 'subfile2', OVERWRITE),
  OUTPUT(isuper);
  FileServices.DeleteLogicalFile(prefix + 'subfile1');
  OUTPUT(isuper);
  FileServices.DeleteLogicalFile(prefix + 'subfile2');
  OUTPUT(isupero);
);

