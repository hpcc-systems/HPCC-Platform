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
//class=spray
//class=copy

import std.system.thorlib;
import Std.File AS FileServices;
import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;
cluster := thorlib.cluster() + '-' + thorlib.platform();

// Super File and Super Key Copy regression test

unsigned VERBOSE := 0;
unsigned CLEAN_UP := 1;

boolean SuperFile := TRUE;
boolean LogicalFile := FALSE;

AlbumRecordDef     := RECORD
    UNSIGNED            Id;
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


albumTable1Name := prefix + 'albums.d01';
albumTable1 := DATASET(albumTable1Name,{AlbumRecordDef,UNSIGNED8 filepos {virtual(fileposition)}},FLAT);

albumTable2Name := prefix + 'albums.d02';
albumTable2 := DATASET(albumTable2Name,{AlbumRecordDef,UNSIGNED8 filepos {virtual(fileposition)}},FLAT);

albumIndex1Name := prefix + 'albums1.key';
albumIndex1 := INDEX(albumTable1(Tracks<=4),{ Artist, Title, filepos }, albumIndex1Name);

albumIndex2Name := prefix + 'albums2.key';
albumIndex2 := INDEX(albumTable1((Tracks>4)AND(Tracks<8)),{ Artist, Title, filepos }, albumIndex2Name);

albumIndex3Name := prefix + 'albums3.key';
albumIndex3  := INDEX(albumTable1,{ Artist, Title, filepos }, albumIndex3Name);

superFile0SubName := prefix + 'superalbums0sub';
superFile0SubLfCopyName := prefix + 'superalbums0sub_lf_copy';
superFile0SubSfCopyName := prefix + 'superalbums0sub_Sf_copy';

superFile1SubName := prefix + 'superalbums1sub';
superFile1SubLfCopyName := prefix + 'superalbums1sub_lf_copy';
superFile1SubSfCopyName := prefix + 'superalbums1sub_Sf_copy';

superFile2SubsName := prefix + 'superalbums2subs';
superFile2SubsLfCopyName := prefix + 'superalbums2subs_lf_copy';
superFile2SubsSfCopyName := prefix + 'superalbums2subs_Sf_copy';


superIndex0SubName := prefix + 'superalbums0sub.key';
superIndex0SubLfCopyName := prefix + 'superalbums0sub.key_lf_copy';
superIndex0SubSfCopyName := prefix + 'superalbums0sub.key_sf_copy';

superIndex1SubName := prefix + 'superalbums1sub.key';
superIndex1SubLfCopyName := prefix + 'superalbums1sub.key_lf_copy';
superIndex1SubSfCopyName := prefix + 'superalbums1sub.key_sf_copy';

superIndex2SubName := prefix + 'superalbums2subs.key';
superIndex2SubsLfCopyName := prefix + 'superalbums2subs.key_lf_copy';
superIndex2SubsSfCopyName := prefix + 'superalbums2subs.key_sf_copy';

superIndex3SubsName := prefix + 'superalbums3subs.key';
superIndex3SubsLfCopyName := prefix + 'superalbums3subs.key_lf_copy';
superIndex3SubsSfCopyName := prefix + 'superalbums3subs.key_sf_copy';

// The rsfile:= FileServices.SuperFileExists('~'+<logical_file_name>); can be used to check target file type, 
// especially for superindex files with <= 1 subfile
// See "gatherCounts(string filename, unsigned origRecCount, boolean noSplit, string operation = 'variable') := FUNCTION" 
// in split_test.ecl (HPCC-21779 PR-12836) 

checkFile(string filename, boolean isSuperfile) := FUNCTION

    isFileExists :=  FileServices.FileExists('~' + filename);
    isSf := if ( isFileExists = TRUE,
                 FileServices.SuperFileExists('~' + filename),
                 FALSE
               ); 
               
    res := if ( isFileExists = FALSE,
                'File not found.',
                if (isSuperfile = isSf,
                    'The file is ok.',
                    'The ' +  filename + ' is expected as ' + if (isSuperfile = TRUE, 'superfile', 'logical file') + ' but it is a ' + if ( isSf = TRUE, 'superfile', 'logical file') + '.'
                    )
              );
              
     RETURN DATASET(ROW(TRANSFORM( { STRING fileCheck }, self.fileCheck := res)));
END;


rec := RECORD
  string sourceFileName;
  string destFileName;
  string destCluster;
  boolean asSuperfile;
  string result;
  string msg;
end;

// Copy
supercopy(string sourceFileName, string destCluster, string destFileName, boolean asSuperFile) := FUNCTION
  RETURN FileServices.fCopy(sourceLogicalName := sourceFileName,
                            destinationGroup := destCluster,
                            destinationLogicalName := destFileName,
                            ASSUPERFILE := asSuperfile,
                            ALLOWOVERWRITE := True
                           );
end;

doSuperCopy(string sourceFileName, string destFileName, string destCluster, boolean asSuperFile, string resultMsg) := FUNCTION
    p1a := PROJECT(DATASET(ROW(TRANSFORM(rec, SELF := []))), TRANSFORM(rec, SELF.msg := supercopy(sourceFileName, destCluster, destFileName, asSuperFile);
                                                                            SELF.result := resultMsg + ' Pass';
                                                                            SELF.sourceFileName := sourceFileName;
                                                                            SELF.destFileName := destFileName;
                                                                            SELF.destCluster := destCluster;
                                                                            SELF.asSuperFile := asSuperFile));
    c1a := CATCH(NOFOLD(p1a), ONFAIL(TRANSFORM(rec, SELF.result := resultMsg + ' Fail',
                                                    SELF.msg := FAILMESSAGE,
                                                    SELF.sourceFileName := sourceFileName;
                                                    SELF.destFileName := destFileName;
                                                    SELF.destCluster := destCluster;
                                                    SELF.asSuperFile := asSuperFile;
                                              )));

    #if (VERBOSE = 1)
        RETURN output(c1a);
    #else
        RETURN output(c1a, {result});
    #end
END;


// Superfile tests
// The target should be a logical file (destination cluster is myroxie to avoid  missing info to repartitioning error)
super0copyOut := doSuperCopy(superFile0SubName, superFile0SubLfCopyName, 'myroxie', LogicalFile, 'Copy superfile with 0 subfile as logical file:');
super0copyOutCheck := checkFile(superFile0SubLfCopyName, LogicalFile);

// The target should be a super file
super0copyOut2 := doSuperCopy(superFile0SubName, superFile0SubSfCopyName, 'mythor', SuperFile, 'Copy superfile with 0 subfile as superfile:');
super0copyOut2Check := checkFile(superFile0SubSfCopyName, SuperFile);



// The target should be a logical file
super1copyOut := doSuperCopy(superFile1SubName, superFile1SubLfCopyName, 'mythor', LogicalFile, 'Copy superfile with 1 subfile as logical file:');
super1copyOutCheck := checkFile(superFile1SubLfCopyName, LogicalFile);

// The target should be a super file
super1copyOut2 := doSuperCopy(superFile1SubName, superFile1SubSfCopyName, 'mythor', SuperFile, 'Copy superfile with 1 subfile  as superfile:');
super1copyOut2Check := checkFile(superFile1SubSfCopyName, SuperFile);



// The target should be a logical file
super2copyOut := doSuperCopy(superFile2SubsName, superFile2SubsLfCopyName, 'mythor', LogicalFile, 'Copy superfile with 2 subfiles as logical file:');
super2copyOutCheck := checkFile(superFile2SubsLfCopyName, LogicalFile);

// The target should be a super file
super2copyOut2 := doSuperCopy(superFile2SubsName, superFile2SubsSfCopyName, 'mythor', SuperFile, 'Copy superfilewith 2 subfiles as superfile:');
super2copyOut2Check := checkFile(superFile2SubsSfCopyName, SuperFile);



// Superindex copy tests

// The target should be a logical file (destination cluster is myroxie to avoid missing info to repartitioning error)
super0copyOut3 := doSuperCopy(superIndex0SubName, superIndex0SubLfCopyName, 'myroxie', LogicalFile, 'Copy superindex with 0 subfile as logical file:');
super0copyOut3Check := checkFile(superIndex0SubLfCopyName, LogicalFile);

// The target should be a superindex file
super0copyOut4 := doSuperCopy(superIndex0SubName, superIndex0SubSfCopyName, 'mythor', SuperFile, 'Copy superindex with 0 subfile as superfile:');
super0copyOut4Check := checkFile(superIndex0SubSfCopyName, SuperFile);



// Actually this forced to copy as SuperFile. After HPCC-22670 Pr-12891 has been merged, 
// the target should be a logical file
super1copyOut3 := doSuperCopy(superIndex1SubName, superIndex1SubLfCopyName, 'mythor', LogicalFile, 'Copy superindex with 1 subfile as logical file:');
super1copyOut3Check := checkFile(superIndex1SubLfCopyName, LogicalFile);

// The target should be a superindex file
super1copyOut4 := doSuperCopy(superIndex1SubName, superIndex1SubSfCopyName, 'mythor', SuperFile, 'Copy superindex with 1 subfile as superfile:');
super1copyOut4Check := checkFile(superIndex1SubSfCopyName, SuperFile);



// The target should (forced to) be a superindex file
super2copyOut3 := doSuperCopy(superIndex2SubName, superIndex2SubsLfCopyName, 'mythor', LogicalFile, 'Copy superindex with 2 subfiles as logical file:');
super2copyOut3Check := checkFile(superIndex2SubsLfCopyName, SuperFile);

// The target should be a superindex file
super2copyOut4 := doSuperCopy(superIndex2SubName, superIndex2SubsSfCopyName, 'mythor', SuperFile, 'Copy superindex with 2 subfiles as superfile:');
super2copyOut4Check := checkFile(superIndex2SubsSfCopyName, SuperFile);



// The target should (forced to) be a superindex file
super3copyOut3 := doSuperCopy(superIndex3SubsName, superIndex3SubsLfCopyName, 'mythor', LogicalFile, 'Copy superindex with 3 subfiles as logical file:');
super3copyOut3Check := checkFile(superIndex3SubsLfCopyName, SuperFile);

// The target should be a superindex file
super3copyOut4 := doSuperCopy(superIndex3SubsName, superIndex3SubsSfCopyName, 'mythor', SuperFile, 'Copy superindex with 3 subfiles as superfile:');
super3copyOut4Check := checkFile(superIndex3SubsSfCopyName, SuperFile);


SEQUENTIAL(

    // Create a logical file and add it to a Superfile
    OUTPUT(ds,,albumTable1Name,OVERWRITE),
    OUTPUT(ds,,albumTable2Name,OVERWRITE),

    // Create a some index files and add them to a Superindex
    BUILDINDEX(albumIndex1,OVERWRITE),
    BUILDINDEX(albumIndex2,OVERWRITE),
    BUILDINDEX(albumIndex3,OVERWRITE),


    // Create superfile without sub-file
    FileServices.CreateSuperFile(superFile0SubName),
  
  
    // Create superfile and add one subfile to it
    FileServices.CreateSuperFile(superFile1SubName),
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile(superFile1SubName,albumTable1Name),
    FileServices.FinishSuperFileTransaction(),
    
    // Create superfile and add two subfiles to it
    FileServices.CreateSuperFile(superFile2SubsName),
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile(superFile2SubsName,albumTable1Name),
    FileServices.AddSuperFile(superFile2SubsName,albumTable2Name),
    FileServices.FinishSuperFileTransaction(),


    // Copy Superfile as a logical file - should pass
    super0copyOut,
    output(super0copyOutCheck, NAMED('CopySuperfileW0SubAsLogicalFileResult')),
    
    // Copy Superfile as a superfile - should pass
    super0copyOut2,
    output(super0copyOut2Check, NAMED('CopySuperfileW0SubAsSuperFileResult')),


    // Copy Superfile as a logical file - should pass
    super1copyOut,
    output(super1copyOutCheck, NAMED('CopySuperfileW1SubAsLogicalFileResult')),
    
    // Copy Superfile as a superfile - should pass
    super1copyOut2,
    output(super1copyOut2Check, NAMED('CopySuperfileW1SubAsSuperFileResult')),


    // Copy Superfile as a logical file - should pass
    super2copyOut,
    output(super2copyOutCheck, NAMED('CopySuperfileW2SubsAsLogicalFileResult')),
    
    // Copy Superfile as a superfile - should pass
    super2copyOut2,
    output(super2copyOut2Check, NAMED('CopySuperfileW2SubsAsSuperFileResult')),

    
    // Create superindex without any sub-index file
    FileServices.CreateSuperFile(superIndex0SubName),

    
    // Create superindex with one sub-index file
    FileServices.CreateSuperFile(superIndex1SubName),
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile(superIndex1SubName, albumIndex1Name),
    FileServices.FinishSuperFileTransaction(),


    // Create superindex with two sub-index files
    FileServices.CreateSuperFile(superIndex2SubName),
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile(superIndex2SubName, albumIndex1Name),
    FileServices.AddSuperFile(superIndex2SubName, albumIndex2Name),
    FileServices.FinishSuperFileTransaction(),
    

    // Create superindex with three sub-index files
    FileServices.CreateSuperFile(superIndex3SubsName),
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile(superIndex3SubsName, albumIndex1Name),
    FileServices.AddSuperFile(superIndex3SubsName, albumIndex2Name),
    FileServices.AddSuperFile(superIndex3SubsName, albumIndex3Name),
    FileServices.FinishSuperFileTransaction(),



    //Copy Superindex as a logical file - should fail
    super0copyOut3;
    output(super0copyOut3Check, NAMED('CopySuperIndexW0SubAsLogicalFileResult')),

    // Copy Superindex as a Super file - should pass
    super0copyOut4;
    output(super0copyOut4Check, NAMED('CopySuperIndexW0SubAsSuperFileResult')),
    

    //Copy Superindex as a logical file - should fail
    super1copyOut3;
    output(super1copyOut3Check, NAMED('CopySuperIndexW1SubAsLogicalFileResult')),

    // Copy Superindex as a Super file - should pass
    super1copyOut4;
    output(super1copyOut4Check, NAMED('CopySuperIndexW1SubAsSuperFileResult')),


    //Copy Superindex as a logical file - should fail
    super2copyOut3;
    output(super2copyOut3Check, NAMED('CopySuperIndexW2SubsAsLogicalFileResult')),

    // Copy Superindex as a Super file - should pass
    super2copyOut4;
    output(super2copyOut4Check, NAMED('CopySuperIndexW2SubsAsSuperFileResult')),
    

    //Copy Superindex as a logical file - should fail
    super3copyOut3;
    output(super3copyOut3Check, NAMED('CopySuperIndexW3SubsAsLogicalFileResult')),

    // Copy Superindex as a Super file - should pass
    super3copyOut4;
    output(super3copyOut4Check, NAMED('CopySuperIndexW3SubsAsSuperFileResult')),


#if (CLEAN_UP = 1)
    // Clean-up
    FileServices.DeleteSuperFile(superFile0SubName),
    FileServices.DeleteLogicalFile(superFile0SubLfCopyName),
    FileServices.DeleteSuperFile(superFile0SubSfCopyName),
    
    FileServices.DeleteSuperFile(superFile1SubName),
    FileServices.DeleteLogicalFile(superFile1SubLfCopyName),
    FileServices.DeleteSuperFile(superFile1SubSfCopyName),
    
    FileServices.DeleteSuperFile(superFile2SubsName),
    FileServices.DeleteLogicalFile(superFile2SubsLfCopyName),
    FileServices.DeleteSuperFile(superFile2SubsSfCopyName),
    
    FileServices.DeleteSuperFile(superIndex0SubName),
    FileServices.DeleteLogicalFile(superIndex0SubLfCopyName),
    FileServices.DeleteSuperFile(superIndex0SubSfCopyName),

    FileServices.DeleteSuperFile(superIndex1SubName),
    FileServices.DeleteLogicalFile(superIndex1SubLfCopyName),
    FileServices.DeleteSuperFile(superIndex1SubSfCopyName),
    
    FileServices.DeleteSuperFile(superIndex2SubName),
    FileServices.DeleteLogicalFile(superIndex2SubsLfCopyName),
    FileServices.DeleteSuperFile(superIndex2SubsSfCopyName),
    
    FileServices.DeleteSuperFile(superIndex3SubsName),
    FileServices.DeleteLogicalFile(superIndex3SubsLfCopyName),
    FileServices.DeleteSuperFile(superIndex3SubsSfCopyName),
   
   
    FileServices.DeleteLogicalFile(albumIndex1Name),
    FileServices.DeleteLogicalFile(albumIndex2Name),
    FileServices.DeleteLogicalFile(albumIndex3Name),

    FileServices.DeleteLogicalFile(albumTable1Name),
    FileServices.DeleteLogicalFile(albumTable2Name)
#end
  
);
