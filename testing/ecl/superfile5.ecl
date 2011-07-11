/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

// Super File regression test
//noRoxie

import Std.System.Thorlib;
import Std.File AS FileServices;
import Std.Str;

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

  

  

