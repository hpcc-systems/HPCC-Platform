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
ds5 := DATASET([{5,'I'}, {5,'J'}, {5,'K'}], rec);

clusterLFNPrefix := thorlib.getExpandLogicalName('regress::');

string stripPrefix(string qlfn) := IF (Str.Find(qlfn, clusterLFNprefix, 1) = 1, Str.FindReplace(qlfn, clusterLFNPrefix, ''), qlfn);


SEQUENTIAL(
  FileServices.DeleteSuperFile('regress::t1_superfile2'),
  FileServices.DeleteSuperFile('regress::t1_superfile1'),
  OUTPUT(ds1,,'regress::t1_subfile1',overwrite),
  OUTPUT(ds2,,'regress::t1_subfile2',overwrite),
  OUTPUT(ds3,,'regress::t1_subfile3',overwrite),
  OUTPUT(ds4,,'regress::t1_subfile4',overwrite),
  OUTPUT(ds5,,'regress::t1_subfile5',overwrite),
  FileServices.CreateSuperFile('regress::t1_superfile1'),
  FileServices.StartSuperFileTransaction(),
  FileServices.AddSuperFile('regress::t1_superfile1','regress::t1_subfile1'),
  FileServices.AddSuperFile('regress::t1_superfile1','regress::t1_subfile2'),
  FileServices.AddSuperFile('regress::t1_superfile1','regress::t1_subfile3'),
  FileServices.AddSuperFile('regress::t1_superfile1','regress::t1_subfile4'),
  FileServices.FinishSuperFileTransaction(),
  FileServices.AddSuperFile('regress::t1_superfile2','regress::t1_subfile4'),
  FileServices.AddSuperFile('regress::t1_superfile2','regress::t1_subfile3'),
  FileServices.AddSuperFile('regress::t1_superfile2','regress::t1_subfile2'),
  FileServices.AddSuperFile('regress::t1_superfile2','regress::t1_subfile1'),
  OUTPUT(dataset ('regress::t1_superfile1', rec, flat)),
  OUTPUT(dataset ('regress::t1_superfile2', rec, flat)),
  OUTPUT(FileServices.GetSuperFileSubCount('regress::t1_superfile1')),
  OUTPUT(FileServices.GetSuperFileSubCount('regress::t1_superfile2')),
  OUTPUT(stripPrefix(FileServices.GetSuperFileSubName('regress::t1_superfile2',2))),
  OUTPUT(FileServices.FindSuperFileSubName('regress::t1_superfile2','regress::t1_subfile3')),
  FileServices.StartSuperFileTransaction(),
  FileServices.RemoveSuperFile('regress::t1_superfile1','regress::t1_subfile2'),
  FileServices.ReplaceSuperFile('regress::t1_superfile2','regress::t1_subfile1','regress::t1_subfile5'),
  FileServices.SwapSuperFile('regress::t1_superfile1','regress::t1_superfile2'),
  FileServices.FinishSuperFileTransaction(true),    // rollback
  OUTPUT(FileServices.GetSuperFileSubCount('regress::t1_superfile1')),
  OUTPUT(FileServices.GetSuperFileSubCount('regress::t1_superfile2')),
  OUTPUT(dataset ('regress::t1_superfile1', rec, flat)),
  OUTPUT(dataset ('regress::t1_superfile2', rec, flat)),
  FileServices.StartSuperFileTransaction(),
  FileServices.RemoveSuperFile('regress::t1_superfile1','regress::t1_subfile2'),
  FileServices.ReplaceSuperFile('regress::t1_superfile2','regress::t1_subfile1','regress::t1_subfile5'),
  FileServices.SwapSuperFile('regress::t1_superfile1','regress::t1_superfile2'),
  FileServices.FinishSuperFileTransaction(),  
  OUTPUT(FileServices.GetSuperFileSubCount('regress::t1_superfile1')),
  OUTPUT(FileServices.GetSuperFileSubCount('regress::t1_superfile2')),
  OUTPUT(dataset ('regress::t1_superfile1', rec, flat)),
  OUTPUT(dataset ('regress::t1_superfile2', rec, flat)),
  FileServices.AddSuperFile('regress::t1_superfile2','regress::t1_superfile1',0),
  OUTPUT(dataset ('regress::t1_superfile2', rec, flat)),
  FileServices.ClearSuperFile('regress::t1_superfile1'),
  OUTPUT(dataset ('regress::t1_superfile2', rec, flat)),
  FileServices.ClearSuperFile('regress::t1_superfile2'),
  FileServices.AddSuperFile('regress::t1_superfile1','regress::t1_subfile1'),
  FileServices.AddSuperFile('regress::t1_superfile1','regress::t1_subfile2'),
  FileServices.AddSuperFile('regress::t1_superfile1','regress::t1_subfile3'),
  FileServices.AddSuperFile('regress::t1_superfile1','regress::t1_subfile4'),
  FileServices.AddSuperFile('regress::t1_superfile2','regress::t1_superfile1',0),
  FileServices.AddSuperFile('regress::t1_superfile2','regress::t1_superfile1',0,true),
  OUTPUT(dataset ('regress::t1_superfile2', rec, flat)),
  FileServices.ClearSuperFile('regress::t1_superfile1'),
  OUTPUT(dataset ('regress::t1_superfile2', rec, flat)),
  FileServices.DeleteSuperFile('regress::t1_superfile2'),
  OUTPUT(FileServices.SuperFileExists('regress::t1_superfile1')),
  FileServices.DeleteSuperFile('regress::t1_superfile1'),
  OUTPUT(FileServices.SuperFileExists('regress::t1_superfile1')),
  // check removal in transaction
  FileServices.CreateSuperFile('regress::t1_superfile1'),
  FileServices.AddSuperFile('regress::t1_superfile1','regress::t1_subfile1'),
  FileServices.StartSuperFileTransaction(),
  FileServices.RemoveSuperFile ('regress::t1_superfile1','regress::t1_subfile1',true),
  FileServices.FinishSuperFileTransaction(true),    // rollback
  OUTPUT(FileServices.GetSuperFileSubCount('regress::t1_superfile1')),
  OUTPUT(FileServices.FileExists('regress::t1_subfile1')),
  FileServices.StartSuperFileTransaction(),
  FileServices.RemoveSuperFile ('regress::t1_superfile1','regress::t1_subfile1',true),
  FileServices.FinishSuperFileTransaction(),    // no rollback
  OUTPUT(FileServices.GetSuperFileSubCount('regress::t1_superfile1')),
  OUTPUT(FileServices.FileExists('regress::t1_subfile1')),
  // check outside of transaction
  FileServices.AddSuperFile('regress::t1_superfile1','regress::t1_subfile2'),
  FileServices.RemoveSuperFile ('regress::t1_superfile1','regress::t1_subfile2',true),
  OUTPUT(FileServices.GetSuperFileSubCount('regress::t1_superfile1')),
  OUTPUT(FileServices.FileExists('regress::t1_subfile2')),
  FileServices.AddSuperFile('regress::t1_superfile2','regress::t1_superfile1'),
  FileServices.AddSuperFile('regress::t1_superfile1','regress::t1_subfile3'),
  FileServices.RemoveSuperFile ('regress::t1_superfile2','regress::t1_superfile1'),
  FileServices.RemoveSuperFile ('regress::t1_superfile1','regress::t1_subfile3',true),
  FileServices.StartSuperFileTransaction(),
  FileServices.AddSuperFile('regress::t1_superfile2','regress::t1_superfile1'),
  FileServices.AddSuperFile('regress::t1_superfile1','regress::t1_subfile4'),
  FileServices.RemoveSuperFile ('regress::t1_superfile1','regress::t1_subfile4',true),
  FileServices.RemoveSuperFile ('regress::t1_superfile2','regress::t1_superfile1'),
  FileServices.FinishSuperFileTransaction(),    // no rollback
  FileServices.DeleteLogicalFile('regress::t1_subfile5')
);

  

  

