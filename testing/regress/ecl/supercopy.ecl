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

#onwarning(4523, ignore);

//nothor
//class=superfile

import Std.File AS FileServices;
import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;
// Super Fiel and Key Copy regression test

unsigned VERBOSE := 0;

AlbumRecordDef     := RECORD
UNSIGNED                        Id;
STRING80            Title;
STRING64            Artist;
UNSIGNED            Tracks;
UNSIGNED            Mins;
UNSIGNED            Secs;
UNSIGNED            Player;
UNSIGNED            Position;
                      END;

ds := DATASET(
[
  {251,'Kingsize','The Boo Radleys',14,63,55,0,252}
], AlbumRecordDef );


albumTableName := prefix + 'albums.d00';
albumTable := DATASET(albumTableName,{AlbumRecordDef,UNSIGNED8 filepos {virtual(fileposition)}},FLAT);

albumIndex1Name := prefix + 'albums1.key';
albumIndex1 := INDEX(albumTable(Tracks<=4),{ Artist, Title, filepos }, albumIndex1Name);

albumIndex2Name := prefix + 'albums2.key';
albumIndex2 := INDEX(albumTable((Tracks>4)AND(Tracks<8)),{ Artist, Title, filepos }, albumIndex2Name);

albumIndexName := prefix + 'albums.key';
albumIndex  := INDEX(albumTable,{ Artist, Title, filepos }, albumIndexName);

superFileName := prefix + 'superalbums';
superFileCopyName := prefix + 'superalbums_copy';

superIndexName := prefix + 'superalbums.key';
superIndexCopyName := prefix + 'superalbums.key_copy';


rec := RECORD
  string sourceFileName;
  string destFileName;
  string destCluster;
  boolean asSuperfile;
  string result;
  string msg;
end;

// Copy
rec supercopy(rec l) := TRANSFORM
  SELF.msg := FileServices.fCopy(
                       sourceLogicalName := l.sourceFileName
                      ,destinationGroup := l.destCluster
                      ,destinationLogicalName := l.destFileName
                      ,ASSUPERFILE := l.asSuperfile
                      ,ALLOWOVERWRITE := True
                      );
  SELF.result := l.result + ' Pass';
  SELF.sourceFileName := l.sourceFileName;
  SELF.destFileName := l.destFileName;
  SELF.destCluster := l.destCluster;
  SELF.asSuperfile := l.asSuperfile;
end;

dst1 := NOFOLD(DATASET([{superFileName, superFileCopyName, 'mythor', False, 'Copy superfile as logical file:', ''}], rec));
p1 := PROJECT(NOFOLD(dst1), supercopy(LEFT));
c1 := CATCH(NOFOLD(p1), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Copy superfile as logical file: Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.sourceFileName := superFileName,
                                 SELF.destFileName := superFileCopyName,
                                 SELF.destCluster := 'mythor',
                                 SELF.asSuperfile := False
                                )));
#if (VERBOSE = 1)
   supercopyOut := output(c1);
#else
   supercopyOut := output(c1, {result});
#end


dst2 := NOFOLD(DATASET([{superFileName, superFileCopyName, 'mythor', True, 'Copy superfile as superfle:', ''}], rec));
p2 := PROJECT(NOFOLD(dst2), supercopy(LEFT));
c2 := CATCH(NOFOLD(p2), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Copy superfile as superfle: Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.sourceFileName := superFileName,
                                 SELF.destFileName := superFileCopyName,
                                 SELF.destCluster := 'mythor',
                                 SELF.asSuperfile := True
                                )));
#if (VERBOSE = 1)
   supercopyOut2 := output(c2);
#else
   supercopyOut2 := output(c2, {result});
#end



dst3 := NOFOLD(DATASET([{superIndexName, superIndexCopyName, 'myroxie', False, 'Copy superindex as logical file:', ''}], rec));
p3 := PROJECT(NOFOLD(dst3), supercopy(LEFT));
c3 := CATCH(NOFOLD(p3), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Copy superindex as logical file: Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.sourceFileName := superIndexName,
                                 SELF.destFileName := superIndexName + '_copy',
                                 SELF.destCluster := 'myroxie',
                                 SELF.asSuperfile := False
                                )));
#if (VERBOSE = 1)
   supercopyOut3 := output(c3);
#else
   supercopyOut3 := output(c3, {result});
#end


dst4 := NOFOLD(DATASET([{superIndexName, superIndexCopyName, 'mythor', True, 'Copy superindex as superfle:', ''}], rec));
p4 := PROJECT(NOFOLD(dst4), supercopy(LEFT));
c4 := CATCH(NOFOLD(p4), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Copy superindex as superfle: Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.sourceFileName := superIndexName,
                                 SELF.destFileName := superIndexCopyName,
                                 SELF.destCluster := 'mythor',
                                 SELF.asSuperfile := True
                                )));
#if (VERBOSE = 1)
   supercopyOut4 := output(c4);
#else
   supercopyOut4 := output(c4, {result});
#end

SEQUENTIAL(

    // Create a logical file and add it to a Superfile
    OUTPUT(ds,,albumTableName,OVERWRITE),

    FileServices.CreateSuperFile(superFileName),
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile(superFileName,albumTableName),
    FileServices.FinishSuperFileTransaction(),

    // Create a some index files and add them to a Superindex
    BUILDINDEX(albumIndex1,OVERWRITE),
    BUILDINDEX(albumIndex2,OVERWRITE),
    BUILDINDEX(albumIndex,OVERWRITE),

    FileServices.CreateSuperFile(superIndexName),
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile(prefix + 'superalbums.key', albumIndex1Name),
    FileServices.AddSuperFile(prefix + 'superalbums.key', albumIndex2Name),
    FileServices.AddSuperFile(prefix + 'superalbums.key', albumIndexName),
    FileServices.FinishSuperFileTransaction(),

    // Copy Superfile as a logical file - should pass
    supercopyOut;
    // Copy Superfile as a superfile - should pass
    supercopyOut2;

    //Copy Superindex as a logical file - should fail
    supercopyOut3;

    // Copy Superindex as a Super file - should pass
    supercopyOut4;

    // Clean-up
    FileServices.DeleteSuperFile(superFileName),
    FileServices.DeleteSuperFile(superFileCopyName),
    FileServices.DeleteSuperFile(superIndexName),
    FileServices.DeleteSuperFile(superIndexCopyName),

    FileServices.DeleteLogicalFile(albumIndex1Name),
    FileServices.DeleteLogicalFile(albumIndex2Name),
    FileServices.DeleteLogicalFile(albumIndexName),

    FileServices.DeleteLogicalFile(albumTableName),
);
