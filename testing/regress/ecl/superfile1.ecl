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
ds5 := DATASET([{5,'I'}, {5,'J'}, {5,'K'}], rec);

clusterLFNPrefix := thorlib.getExpandLogicalName(prefix);

string stripPrefix(string qlfn) := IF (Str.Find(qlfn, clusterLFNprefix, 1) = 1, Str.FindReplace(qlfn, clusterLFNPrefix, ''), qlfn);


SEQUENTIAL(
  FileServices.DeleteSuperFile(prefix + 't1_superfile2'),
  FileServices.DeleteSuperFile(prefix + 't1_superfile1'),
  OUTPUT(ds1,,prefix + 't1_subfile1',overwrite),
  OUTPUT(ds2,,prefix + 't1_subfile2',overwrite),
  OUTPUT(ds3,,prefix + 't1_subfile3',overwrite),
  OUTPUT(ds4,,prefix + 't1_subfile4',overwrite),
  OUTPUT(ds5,,prefix + 't1_subfile5',overwrite),
  FileServices.CreateSuperFile(prefix + 't1_superfile1'),
  FileServices.StartSuperFileTransaction(),
  FileServices.AddSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile1'),
  FileServices.AddSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile2'),
  FileServices.AddSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile3'),
  FileServices.AddSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile4'),
  FileServices.FinishSuperFileTransaction(),
  FileServices.AddSuperFile(prefix + 't1_superfile2',prefix + 't1_subfile4'),
  FileServices.AddSuperFile(prefix + 't1_superfile2',prefix + 't1_subfile3'),
  FileServices.AddSuperFile(prefix + 't1_superfile2',prefix + 't1_subfile2'),
  FileServices.AddSuperFile(prefix + 't1_superfile2',prefix + 't1_subfile1'),
  OUTPUT(dataset (prefix + 't1_superfile1', rec, flat)),
  OUTPUT(dataset (prefix + 't1_superfile2', rec, flat)),
  OUTPUT(FileServices.GetSuperFileSubCount(prefix + 't1_superfile1')),
  OUTPUT(FileServices.GetSuperFileSubCount(prefix + 't1_superfile2')),
  OUTPUT(stripPrefix(FileServices.GetSuperFileSubName(prefix + 't1_superfile2',2))),
  OUTPUT(FileServices.FindSuperFileSubName(prefix + 't1_superfile2',prefix + 't1_subfile3')),
  FileServices.StartSuperFileTransaction(),
  FileServices.RemoveSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile2'),
  FileServices.ReplaceSuperFile(prefix + 't1_superfile2',prefix + 't1_subfile1',prefix + 't1_subfile5'),
  FileServices.SwapSuperFile(prefix + 't1_superfile1',prefix + 't1_superfile2'),
  FileServices.FinishSuperFileTransaction(true),    // rollback
  OUTPUT(FileServices.GetSuperFileSubCount(prefix + 't1_superfile1')),
  OUTPUT(FileServices.GetSuperFileSubCount(prefix + 't1_superfile2')),
  OUTPUT(dataset (prefix + 't1_superfile1', rec, flat)),
  OUTPUT(dataset (prefix + 't1_superfile2', rec, flat)),
  FileServices.StartSuperFileTransaction(),
  FileServices.RemoveSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile2'),
  FileServices.ReplaceSuperFile(prefix + 't1_superfile2',prefix + 't1_subfile1',prefix + 't1_subfile5'),
  FileServices.SwapSuperFile(prefix + 't1_superfile1',prefix + 't1_superfile2'),
  FileServices.FinishSuperFileTransaction(),  
  OUTPUT(FileServices.GetSuperFileSubCount(prefix + 't1_superfile1')),
  OUTPUT(FileServices.GetSuperFileSubCount(prefix + 't1_superfile2')),
  OUTPUT(dataset (prefix + 't1_superfile1', rec, flat)),
  OUTPUT(dataset (prefix + 't1_superfile2', rec, flat)),
  FileServices.AddSuperFile(prefix + 't1_superfile2',prefix + 't1_superfile1',0),
  OUTPUT(dataset (prefix + 't1_superfile2', rec, flat)),
  FileServices.ClearSuperFile(prefix + 't1_superfile1'),
  OUTPUT(dataset (prefix + 't1_superfile2', rec, flat)),
  FileServices.ClearSuperFile(prefix + 't1_superfile2'),
  FileServices.AddSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile1'),
  FileServices.AddSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile2'),
  FileServices.AddSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile3'),
  FileServices.AddSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile4'),
  FileServices.AddSuperFile(prefix + 't1_superfile2',prefix + 't1_superfile1',0),
  FileServices.AddSuperFile(prefix + 't1_superfile2',prefix + 't1_superfile1',0,true),
  OUTPUT(dataset (prefix + 't1_superfile2', rec, flat)),
  FileServices.ClearSuperFile(prefix + 't1_superfile1'),
  OUTPUT(dataset (prefix + 't1_superfile2', rec, flat)),
  FileServices.DeleteSuperFile(prefix + 't1_superfile2'),
  OUTPUT(FileServices.SuperFileExists(prefix + 't1_superfile1')),
  FileServices.DeleteSuperFile(prefix + 't1_superfile1'),
  OUTPUT(FileServices.SuperFileExists(prefix + 't1_superfile1')),
  // check removal in transaction
  FileServices.CreateSuperFile(prefix + 't1_superfile1'),
  FileServices.AddSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile1'),
  FileServices.StartSuperFileTransaction(),
  FileServices.RemoveSuperFile (prefix + 't1_superfile1',prefix + 't1_subfile1',true),
  FileServices.FinishSuperFileTransaction(true),    // rollback
  OUTPUT(FileServices.GetSuperFileSubCount(prefix + 't1_superfile1')),
  OUTPUT(FileServices.FileExists(prefix + 't1_subfile1')),
  FileServices.StartSuperFileTransaction(),
  FileServices.RemoveSuperFile (prefix + 't1_superfile1',prefix + 't1_subfile1',true),
  FileServices.FinishSuperFileTransaction(),    // no rollback
  OUTPUT(FileServices.GetSuperFileSubCount(prefix + 't1_superfile1')),
  OUTPUT(FileServices.FileExists(prefix + 't1_subfile1')),
  // check outside of transaction
  FileServices.AddSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile2'),
  FileServices.RemoveSuperFile (prefix + 't1_superfile1',prefix + 't1_subfile2',true),
  OUTPUT(FileServices.GetSuperFileSubCount(prefix + 't1_superfile1')),
  OUTPUT(FileServices.FileExists(prefix + 't1_subfile2')),
  FileServices.AddSuperFile(prefix + 't1_superfile2',prefix + 't1_superfile1'),
  FileServices.AddSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile3'),
  FileServices.RemoveSuperFile (prefix + 't1_superfile2',prefix + 't1_superfile1'),
  FileServices.RemoveSuperFile (prefix + 't1_superfile1',prefix + 't1_subfile3',true),
  FileServices.StartSuperFileTransaction(),
  FileServices.AddSuperFile(prefix + 't1_superfile2',prefix + 't1_superfile1'),
  FileServices.AddSuperFile(prefix + 't1_superfile1',prefix + 't1_subfile4'),
  FileServices.RemoveSuperFile (prefix + 't1_superfile1',prefix + 't1_subfile4',true),
  FileServices.RemoveSuperFile (prefix + 't1_superfile2',prefix + 't1_superfile1'),
  FileServices.FinishSuperFileTransaction(),    // no rollback
  FileServices.DeleteLogicalFile(prefix + 't1_subfile5')
);

  

  

