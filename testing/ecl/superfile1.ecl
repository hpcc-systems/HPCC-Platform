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
//noRoxie

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
  FileServices.DeleteSuperFile('regress::superfile2'),
  FileServices.DeleteSuperFile('regress::superfile1'),
  OUTPUT(ds1,,'regress::subfile1',overwrite),
  OUTPUT(ds2,,'regress::subfile2',overwrite),
  OUTPUT(ds3,,'regress::subfile3',overwrite),
  OUTPUT(ds4,,'regress::subfile4',overwrite),
  OUTPUT(ds5,,'regress::subfile5',overwrite),
  FileServices.CreateSuperFile('regress::superfile1'),
  FileServices.StartSuperFileTransaction(),
  FileServices.AddSuperFile('regress::superfile1','regress::subfile1'),
  FileServices.AddSuperFile('regress::superfile1','regress::subfile2'),
  FileServices.AddSuperFile('regress::superfile1','regress::subfile3'),
  FileServices.AddSuperFile('regress::superfile1','regress::subfile4'),
  FileServices.FinishSuperFileTransaction(),
  FileServices.AddSuperFile('regress::superfile2','regress::subfile4'),
  FileServices.AddSuperFile('regress::superfile2','regress::subfile3'),
  FileServices.AddSuperFile('regress::superfile2','regress::subfile2'),
  FileServices.AddSuperFile('regress::superfile2','regress::subfile1'),
  OUTPUT(dataset ('regress::superfile1', rec, flat)),
  OUTPUT(dataset ('regress::superfile2', rec, flat)),
  OUTPUT(FileServices.GetSuperFileSubCount('regress::superfile1')),
  OUTPUT(FileServices.GetSuperFileSubCount('regress::superfile2')),
  OUTPUT(stripPrefix(FileServices.GetSuperFileSubName('regress::superfile2',2))),       
  OUTPUT(FileServices.FindSuperFileSubName('regress::superfile2','regress::subfile3')), 
  FileServices.StartSuperFileTransaction(),
  FileServices.RemoveSuperFile('regress::superfile1','regress::subfile2'),
  FileServices.ReplaceSuperFile('regress::superfile2','regress::subfile1','regress::subfile5'),
  FileServices.SwapSuperFile('regress::superfile1','regress::superfile2'),
  FileServices.FinishSuperFileTransaction(true),    // rollback
  OUTPUT(FileServices.GetSuperFileSubCount('regress::superfile1')),
  OUTPUT(FileServices.GetSuperFileSubCount('regress::superfile2')),
  OUTPUT(dataset ('regress::superfile1', rec, flat)),
  OUTPUT(dataset ('regress::superfile2', rec, flat)),
  FileServices.StartSuperFileTransaction(),
  FileServices.RemoveSuperFile('regress::superfile1','regress::subfile2'),
  FileServices.ReplaceSuperFile('regress::superfile2','regress::subfile1','regress::subfile5'),
  FileServices.SwapSuperFile('regress::superfile1','regress::superfile2'),
  FileServices.FinishSuperFileTransaction(),  
  OUTPUT(FileServices.GetSuperFileSubCount('regress::superfile1')),
  OUTPUT(FileServices.GetSuperFileSubCount('regress::superfile2')),
  OUTPUT(dataset ('regress::superfile1', rec, flat)),
  OUTPUT(dataset ('regress::superfile2', rec, flat)),
  FileServices.AddSuperFile('regress::superfile2','regress::superfile1',0),
  OUTPUT(dataset ('regress::superfile2', rec, flat)),
  FileServices.ClearSuperFile('regress::superfile1'),
  OUTPUT(dataset ('regress::superfile2', rec, flat)),
  FileServices.ClearSuperFile('regress::superfile2'),
  FileServices.AddSuperFile('regress::superfile1','regress::subfile1'),
  FileServices.AddSuperFile('regress::superfile1','regress::subfile2'),
  FileServices.AddSuperFile('regress::superfile1','regress::subfile3'),
  FileServices.AddSuperFile('regress::superfile1','regress::subfile4'),
  FileServices.AddSuperFile('regress::superfile2','regress::superfile1',0),
  FileServices.AddSuperFile('regress::superfile2','regress::superfile1',0,true),
  OUTPUT(dataset ('regress::superfile2', rec, flat)),
  FileServices.ClearSuperFile('regress::superfile1'),
  OUTPUT(dataset ('regress::superfile2', rec, flat)),
  FileServices.DeleteSuperFile('regress::superfile2'),
  OUTPUT(FileServices.SuperFileExists('regress::superfile1')),
  FileServices.DeleteSuperFile('regress::superfile1'),
  OUTPUT(FileServices.SuperFileExists('regress::superfile1')),
  // check removal in transaction
  FileServices.CreateSuperFile('regress::superfile1'),
  FileServices.AddSuperFile('regress::superfile1','regress::subfile1'),
  FileServices.StartSuperFileTransaction(),
  FileServices.RemoveSuperFile ('regress::superfile1','regress::subfile1',true), 
  FileServices.FinishSuperFileTransaction(true),    // rollback
  OUTPUT(FileServices.GetSuperFileSubCount('regress::superfile1')),
  OUTPUT(FileServices.FileExists('regress::subfile1')),
  FileServices.StartSuperFileTransaction(),
  FileServices.RemoveSuperFile ('regress::superfile1','regress::subfile1',true), 
  FileServices.FinishSuperFileTransaction(),    // no rollback
  OUTPUT(FileServices.GetSuperFileSubCount('regress::superfile1')),
  OUTPUT(FileServices.FileExists('regress::subfile1')),
  // check outside of transaction
  FileServices.AddSuperFile('regress::superfile1','regress::subfile2'),
  FileServices.RemoveSuperFile ('regress::superfile1','regress::subfile2',true), 
  OUTPUT(FileServices.GetSuperFileSubCount('regress::superfile1')),
  OUTPUT(FileServices.FileExists('regress::subfile2')),
  FileServices.AddSuperFile('regress::superfile2','regress::superfile1'),
  FileServices.AddSuperFile('regress::superfile1','regress::subfile3'),
  FileServices.RemoveSuperFile ('regress::superfile2','regress::superfile1'), 
  FileServices.RemoveSuperFile ('regress::superfile1','regress::subfile3',true), 
  FileServices.StartSuperFileTransaction(),
  FileServices.AddSuperFile('regress::superfile2','regress::superfile1'),
  FileServices.AddSuperFile('regress::superfile1','regress::subfile4'),
  FileServices.RemoveSuperFile ('regress::superfile1','regress::subfile4',true), 
  FileServices.RemoveSuperFile ('regress::superfile2','regress::superfile1'), 
  FileServices.FinishSuperFileTransaction(),    // no rollback
  FileServices.DeleteLogicalFile('regress::subfile5')
);

  

  

