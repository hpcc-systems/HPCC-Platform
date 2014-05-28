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

// Super File regression test

import Std.System.Thorlib;
import Std.File AS FileServices;
import Std.Str;

#option('slaveDaliClient', true);

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

FileServices.FsLogicalFileNameRecord stripTransform(FileServices.FsLogicalFileNameRecord rec) := 
    TRANSFORM
        SELF.name := stripPrefix(rec.name);
    END;

dataset(FileServices.FsLogicalFileNameRecord) stripPrefixList(dataset(FileServices.FsLogicalFileNameRecord) inds) := PROJECT(inds,stripTransform(LEFT));

SEQUENTIAL(
  FileServices.DeleteSuperFile('regress::t5_superfile1'),
  FileServices.DeleteSuperFile('regress::t5_superfile2'),
  FileServices.DeleteSuperFile('regress::t5_superfile3'),
  FileServices.DeleteSuperFile('regress::t5_superfile4'),
  FileServices.DeleteSuperFile('regress::t5_superfile5'),
  OUTPUT(ds1,,'regress::t5_subfile1',overwrite),
  OUTPUT(ds2,,'regress::t5_subfile2',overwrite),
  OUTPUT(ds3,,'regress::t5_subfile3',overwrite),
  OUTPUT(ds4,,'regress::t5_subfile4',overwrite),
  OUTPUT(ds5,,'regress::t5_subfile5',overwrite),
  FileServices.PromoteSuperFileList(['regress::t5_superfile1','regress::t5_superfile2','regress::t5_superfile3','regress::t5_superfile4'],'regress::t5_subfile1'),
  FileServices.PromoteSuperFileList(['regress::t5_superfile1','regress::t5_superfile2','regress::t5_superfile3','regress::t5_superfile4'],'regress::t5_subfile2'),
  FileServices.PromoteSuperFileList(['regress::t5_superfile1','regress::t5_superfile2','regress::t5_superfile3','regress::t5_superfile4'],'regress::t5_subfile3'),
  FileServices.PromoteSuperFileList(['regress::t5_superfile1','regress::t5_superfile2','regress::t5_superfile3','regress::t5_superfile4'],'regress::t5_subfile4'),
  FileServices.PromoteSuperFileList(['regress::t5_superfile1','regress::t5_superfile2','regress::t5_superfile3','regress::t5_superfile4'],'regress::t5_subfile5,regress::t5_subfile1'),
  OUTPUT(stripPrefixList(FileServices.SuperFileContents('regress::t5_superfile1')), NAMED('SF5_RES1')),
  OUTPUT(stripPrefixList(FileServices.SuperFileContents('regress::t5_superfile2')), NAMED('SF5_RES2')),
  OUTPUT(stripPrefixList(FileServices.SuperFileContents('regress::t5_superfile3')), NAMED('SF5_RES3')),
  OUTPUT(stripPrefixList(FileServices.SuperFileContents('regress::t5_superfile4')), NAMED('SF5_RES4')),
  FileServices.CreateSuperFile('regress::t5_superfile5');
  FileServices.AddSuperFile ('regress::t5_superfile5', 'regress::t5_subfile1'),
  FileServices.CreateSuperFile('regress::t5_superfile5',,true);
  FileServices.DeleteSuperFile('regress::t5_superfile5');
  FileServices.DeleteLogicalFile('xyzzy',true);
  FileServices.PromoteSuperFileList(['regress::t5_superfile1','regress::t5_superfile2','regress::t5_superfile3','regress::t5_superfile4'],,true),
  OUTPUT(ds2,,'regress::t5_subfile2'),
  FileServices.PromoteSuperFileList(['regress::t5_superfile1','regress::t5_superfile2','regress::t5_superfile3','regress::t5_superfile4'],'regress::t5_subfile2',true),
  OUTPUT(ds3,,'regress::t5_subfile3'),
  FileServices.PromoteSuperFileList(['regress::t5_superfile1','regress::t5_superfile2','regress::t5_superfile3','regress::t5_superfile4'],'regress::t5_subfile3',true),
  OUTPUT(ds4,,'regress::t5_subfile4'),
  FileServices.PromoteSuperFileList(['regress::t5_superfile1','regress::t5_superfile2','regress::t5_superfile3','regress::t5_superfile4'],'regress::t5_subfile4',true),
  OUTPUT(ds1,,'regress::t5_subfile1'),
  OUTPUT(ds5,,'regress::t5_subfile5'),
  OUTPUT(stripPrefixList(FileServices.SuperFileContents('regress::t5_superfile1')), NAMED('SF5_RES5')),
  OUTPUT(stripPrefixList(FileServices.SuperFileContents('regress::t5_superfile2')), NAMED('SF5_RES6')),
  OUTPUT(stripPrefixList(FileServices.SuperFileContents('regress::t5_superfile3')), NAMED('SF5_RES7')),
  OUTPUT(stripPrefixList(FileServices.SuperFileContents('regress::t5_superfile4')), NAMED('SF5_RES8')),
  FileServices.PromoteSuperFileList(['regress::t5_superfile1','regress::t5_superfile2','regress::t5_superfile3','regress::t5_superfile4'],,false,reverse:=true),
  FileServices.PromoteSuperFileList(['regress::t5_superfile1','regress::t5_superfile2','regress::t5_superfile3','regress::t5_superfile4'],,true,reverse:=true),
  OUTPUT(stripPrefixList(FileServices.SuperFileContents('regress::t5_superfile1')), NAMED('SF5_RES9')),
  OUTPUT(stripPrefixList(FileServices.SuperFileContents('regress::t5_superfile2')), NAMED('SF5_RES10')),
  OUTPUT(stripPrefixList(FileServices.SuperFileContents('regress::t5_superfile3')), NAMED('SF5_RES11')),
  OUTPUT(stripPrefixList(FileServices.SuperFileContents('regress::t5_superfile4')), NAMED('SF5_RES12')),
  OUTPUT(FileServices.GetSuperFileSubCount('regress::t5_superfile1'), NAMED('SF5_RES13')),
  OUTPUT(FileServices.GetSuperFileSubCount('regress::t5_superfile2'), NAMED('SF5_RES14')),
  OUTPUT(stripPrefix(FileServices.GetSuperFileSubName('regress::t5_superfile1',1)), NAMED('SF5_RES15')),
  OUTPUT(stripPrefix(FileServices.GetSuperFileSubName('regress::t5_superfile2',1)), NAMED('SF5_RES16')),
  OUTPUT(stripPrefixList(FileServices.LogicalFileSuperOwners('regress::t5_subfile1')), NAMED('SF5_RES17')),
  OUTPUT(stripPrefixList(FileServices.LogicalFileSuperOwners('regress::t5_subfile2')), NAMED('SF5_RES18')),
  OUTPUT('Done')
);

  

  

