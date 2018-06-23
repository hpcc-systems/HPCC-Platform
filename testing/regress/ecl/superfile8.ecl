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

import Std.System.Thorlib;
import Std.File AS FileServices;
import Std.Str;
import $.setup;
prefix := setup.Files(false, false).FilePrefix;

// Super File regression test

rec :=
RECORD
        integer i;
    string1 id;
END;

ds1 := DATASET([{1,'A'}, {1,'B'}, {1,'C'}], rec);
ds2 := DATASET([{2,'D'}, {2,'E'}], rec);
ds3 := DATASET([{3,'F'}, {3,'G'}, {3,'H'}], rec);
ds4 := DATASET([],rec);

SEQUENTIAL(
  // Prepare
  FileServices.DeleteSuperFile(prefix + 'superfile8'),
  OUTPUT(ds1,,prefix + 'subfile5',overwrite),
  OUTPUT(ds2,,prefix + 'subfile6',overwrite),
  OUTPUT(ds3,,prefix + 'subfile7',overwrite),
  OUTPUT(ds4,,prefix + 'subfile8',overwrite),

  // Delete Super + Rollback (keep subs)
  FileServices.StartSuperFileTransaction(),
  FileServices.CreateSuperFile(prefix + 'superfile8'),
  FileServices.AddSuperFile(prefix + 'superfile8',prefix + 'subfile5'),
  FileServices.AddSuperFile(prefix + 'superfile8',prefix + 'subfile6'),
  FileServices.AddSuperFile(prefix + 'superfile8',prefix + 'subfile7'),
  FileServices.AddSuperFile(prefix + 'superfile8',prefix + 'subfile8'),
  FileServices.DeleteSuperFile(prefix + 'superfile8'),
  FileServices.FinishSuperFileTransaction(true),    // rollback
  OUTPUT(FileServices.SuperFileExists(prefix + 'superfile8')), // false
  OUTPUT(FileServices.FileExists(prefix + 'subfile5')), // true
  OUTPUT(FileServices.FileExists(prefix + 'subfile6')), // true
  OUTPUT(FileServices.FileExists(prefix + 'subfile7')), // true
  OUTPUT(FileServices.FileExists(prefix + 'subfile8')), // true

  // Delete Super + Rollback (del subs, not really)
  FileServices.StartSuperFileTransaction(),
  FileServices.CreateSuperFile(prefix + 'superfile8'),
  FileServices.AddSuperFile(prefix + 'superfile8',prefix + 'subfile5'),
  FileServices.AddSuperFile(prefix + 'superfile8',prefix + 'subfile6'),
  FileServices.AddSuperFile(prefix + 'superfile8',prefix + 'subfile7'),
  FileServices.AddSuperFile(prefix + 'superfile8',prefix + 'subfile8'),
  FileServices.DeleteSuperFile(prefix + 'superfile8', true),
  FileServices.FinishSuperFileTransaction(true),    // rollback
  OUTPUT(FileServices.SuperFileExists(prefix + 'superfile8')), // false
  OUTPUT(FileServices.FileExists(prefix + 'subfile5')), // true
  OUTPUT(FileServices.FileExists(prefix + 'subfile6')), // true
  OUTPUT(FileServices.FileExists(prefix + 'subfile7')), // true
  OUTPUT(FileServices.FileExists(prefix + 'subfile8')), // true

  // Delete Super + Commit (del subs, yes really)
  FileServices.StartSuperFileTransaction(),
  FileServices.CreateSuperFile(prefix + 'superfile8'),
  FileServices.AddSuperFile(prefix + 'superfile8',prefix + 'subfile5'),
  FileServices.AddSuperFile(prefix + 'superfile8',prefix + 'subfile6'),
  FileServices.AddSuperFile(prefix + 'superfile8',prefix + 'subfile7'),
  FileServices.AddSuperFile(prefix + 'superfile8',prefix + 'subfile8'),
  FileServices.DeleteSuperFile(prefix + 'superfile8', true), // del subs
  FileServices.FinishSuperFileTransaction(),        // commit
  OUTPUT(FileServices.SuperFileExists(prefix + 'superfile8')), // false
  OUTPUT(FileServices.FileExists(prefix + 'subfile5')), // false
  OUTPUT(FileServices.FileExists(prefix + 'subfile6')), // false
  OUTPUT(FileServices.FileExists(prefix + 'subfile7')), // false
  OUTPUT(FileServices.FileExists(prefix + 'subfile8')), // false
);
