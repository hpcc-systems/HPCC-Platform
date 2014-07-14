/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

import Std.System.Thorlib;
import Std.File AS FileServices;
import Std.Str;
// Super File regression test

rec :=
RECORD
        integer i;
    string1 id;
END;

ds1 := DATASET([{1,'A'}, {1,'B'}, {1,'C'}], rec);
ds2 := DATASET([{1,'A'}, {1,'B'}, {1,'C'}], rec);

clusterLFNPrefix := thorlib.getExpandLogicalName('regress::');

string stripPrefix(string qlfn) := IF (Str.Find(qlfn, clusterLFNprefix, 1) = 1, Str.FindReplace(qlfn, clusterLFNPrefix, ''), qlfn);

conditionalDelete(string lfn) := FUNCTION
        RETURN IF(FileServices.FileExists(lfn), FileServices.DeleteLogicalFile(lfn));
END;


SEQUENTIAL(
  // Prepare
  conditionalDelete ('regress::subfile12'),
  conditionalDelete ('regress::subfile13'),
  OUTPUT(ds1,,'regress::subfile10',overwrite),
  OUTPUT(ds2,,'regress::subfile11',overwrite),
  OUTPUT(FileServices.FileExists('regress::subfile10')), // true
  OUTPUT(FileServices.FileExists('regress::subfile11')), // true
  OUTPUT(FileServices.FileExists('regress::subfile12')), // false
  OUTPUT(FileServices.FileExists('regress::subfile13')), // false

  // Rename Auto-commit
  FileServices.RenameLogicalFile('regress::subfile10','regress::subfile12'),
  OUTPUT(FileServices.FileExists('regress::subfile10')), // false
  OUTPUT(FileServices.FileExists('regress::subfile12')), // true

  // Rename + Rollback
  FileServices.StartSuperFileTransaction(),
  FileServices.RenameLogicalFile('regress::subfile11','regress::subfile13'),
  FileServices.FinishSuperFileTransaction(true),    // rollback
  OUTPUT(FileServices.FileExists('regress::subfile11')), // true
  OUTPUT(FileServices.FileExists('regress::subfile13')), // false

  // Rename + Commit
  FileServices.StartSuperFileTransaction(),
  FileServices.RenameLogicalFile('regress::subfile11','regress::subfile13'),
  FileServices.FinishSuperFileTransaction(),    // commit
  OUTPUT(FileServices.FileExists('regress::subfile11')), // false
  OUTPUT(FileServices.FileExists('regress::subfile13')), // true
);
