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
ds2 := DATASET([{2,'D'}, {2,'E'}], rec);
ds3 := DATASET([{3,'F'}, {3,'G'}, {3,'H'}], rec);
ds4 := DATASET([],rec);

clusterLFNPrefix := thorlib.getExpandLogicalName('regress::');

string stripPrefix(string qlfn) := IF (Str.Find(qlfn, clusterLFNprefix, 1) = 1, Str.FindReplace(qlfn, clusterLFNPrefix, ''), qlfn);


SEQUENTIAL(
  // Prepare
  FileServices.DeleteSuperFile('regress::superfile7'),
  OUTPUT(ds1,,'regress::subfile1',overwrite),
  OUTPUT(ds2,,'regress::subfile2',overwrite),
  OUTPUT(ds3,,'regress::subfile3',overwrite),
  OUTPUT(ds4,,'regress::subfile4',overwrite),
  FileServices.StartSuperFileTransaction(),
  FileServices.CreateSuperFile('regress::superfile7'),
  FileServices.AddSuperFile('regress::superfile7','regress::subfile1'),
  FileServices.AddSuperFile('regress::superfile7','regress::subfile2'),
  FileServices.AddSuperFile('regress::superfile7','regress::subfile3'),
  FileServices.AddSuperFile('regress::superfile7','regress::subfile4'),
  FileServices.FinishSuperFileTransaction(),
  OUTPUT(FileServices.SuperFileExists('regress::superfile7')), // true
  OUTPUT(FileServices.GetSuperFileSubCount('regress::superfile7')), // 4
  OUTPUT(dataset ('regress::superfile7', rec, flat)),

  // Delete Super + Rollback (keep subs)
  FileServices.StartSuperFileTransaction(),
  FileServices.DeleteSuperFile('regress::superfile7'),
  FileServices.FinishSuperFileTransaction(true),    // rollback
  OUTPUT(FileServices.SuperFileExists('regress::superfile7')), // true
  OUTPUT(FileServices.GetSuperFileSubCount('regress::superfile7')), // 4
  OUTPUT(dataset ('regress::superfile7', rec, flat)),

  // Delete Super + Rollback (del subs, not really)
  FileServices.StartSuperFileTransaction(),
  FileServices.DeleteSuperFile('regress::superfile7', true),
  FileServices.FinishSuperFileTransaction(true),    // rollback
  OUTPUT(FileServices.SuperFileExists('regress::superfile7')), // true
  OUTPUT(FileServices.FileExists('regress::subfile1')), // true
  OUTPUT(FileServices.FileExists('regress::subfile2')), // true
  OUTPUT(FileServices.FileExists('regress::subfile3')), // true
  OUTPUT(FileServices.FileExists('regress::subfile4')), // true
  OUTPUT(FileServices.GetSuperFileSubCount('regress::superfile7')), // 4
  OUTPUT(dataset ('regress::superfile7', rec, flat)),

  // Delete Super + Commit (del subs, yes really)
  FileServices.StartSuperFileTransaction(),
  FileServices.DeleteSuperFile('regress::superfile7', true), // del subs
  FileServices.FinishSuperFileTransaction(),        // commit
  OUTPUT(FileServices.SuperFileExists('regress::superfile7')), // false
  OUTPUT(FileServices.FileExists('regress::subfile1')), // false
  OUTPUT(FileServices.FileExists('regress::subfile2')), // false
  OUTPUT(FileServices.FileExists('regress::subfile3')), // false
  OUTPUT(FileServices.FileExists('regress::subfile4')), // false
);
