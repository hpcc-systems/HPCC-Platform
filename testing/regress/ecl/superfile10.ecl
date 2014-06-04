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
  conditionalDelete ('regress::subfile14'),
  conditionalDelete ('regress::subfile15'),
  OUTPUT(ds1,,'regress::subfile14',overwrite),
  OUTPUT(ds2,,'regress::subfile15',overwrite),
  OUTPUT(FileServices.FileExists('regress::subfile14')), // true
  OUTPUT(FileServices.FileExists('regress::subfile15')), // true

  // Remove Auto-commit
  FileServices.DeleteLogicalFile('regress::subfile14'),
  OUTPUT(FileServices.FileExists('regress::subfile14')), // false
  OUTPUT(FileServices.FileExists('regress::subfile15')), // true

  // Remove + Rollback
  FileServices.StartSuperFileTransaction(),
  FileServices.DeleteLogicalFile('regress::subfile15'),
  FileServices.FinishSuperFileTransaction(true),    // rollback
  OUTPUT(FileServices.FileExists('regress::subfile14')), // false
  OUTPUT(FileServices.FileExists('regress::subfile15')), // true

  // Remove + Commit
  FileServices.StartSuperFileTransaction(),
  FileServices.DeleteLogicalFile('regress::subfile15'),
  FileServices.FinishSuperFileTransaction(),    // commit
  OUTPUT(FileServices.FileExists('regress::subfile14')), // false
  OUTPUT(FileServices.FileExists('regress::subfile15')), // false
);
