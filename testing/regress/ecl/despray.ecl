/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################# */

// The aim of this code is to test the Dropzone Restriction feature implemented in DFU server and
// in some cases (e.g. path points outside of the DZ) it forces the DFU server to throw exeption.
//
// NOTHOR() effectively forces something to be executed globally.  At the moment if a global operation
// fails then the query fails - rather than continuing and only failing if the result of using that
// operation causes a failure.
// So, to avoid to abort this code is excluded from Thor target
//nothor

//nohthor
//class=spray

import $.setup;
import Std.File AS FileServices;


dropzonePath := '/var/lib/HPCCSystems/mydropzone/' : STORED('dropzonePath');
prefix := setup.Files(false, false).QueryFilePrefix;

unsigned VERBOSE := 0;

Layout_Person := RECORD
  UNSIGNED1 PersonID;
  STRING15  FirstName;
  STRING25  LastName;
END;

allPeople := DATASET([ {1,'Fred','Smith'},
                       {2,'Joe','Blow'},
                       {3,'Jane','Smith'}],Layout_Person);

SrcAddrIp := '.';
SrcAddrLocalhost := 'localhost';
File := 'persons';
SourceFile := prefix + File;
DestFileName := WORKUNIT + '-' + File;

//  Outputs  ---
setupPeople := output(allPeople, , SourceFile, OVERWRITE);

ClusterName := 'mythor';

rec := RECORD
  string sourceFile;
  string destFile;
  string ip;
  boolean allowOverwrite;
  string result;
  string msg;
end;

rec t(rec l) := TRANSFORM
  SELF.sourceFile := l.sourceFile;
  SELF.msg := FileServices.fDespray(l.sourceFile
                                         ,l.ip
                                         ,destinationPath := l.destFile
                                         ,ALLOWOVERWRITE := l.allowOverwrite
                                         );
  SELF.result := 'Pass';
  SELF.ip := l.ip;
  SELF.allowOverwrite := l.allowOverwrite;
  SELF.destFile := l.destFile;
end;

// Target path validation checking


// This should be fine based on valid target file path and SrcAddIp
DestFile1 := dropzonePath + DestFileName;
dst2 := NOFOLD(DATASET([{SourceFile, DestFile1, SrcAddrIp, True, '', ''}], rec));

p2 := PROJECT(NOFOLD(dst2), t(LEFT));

c2 := CATCH(NOFOLD(p2), ONFAIL(TRANSFORM(rec,
                                 SELF.sourceFile := SourceFile,
                                 SELF.destFile := DestFile1,
                                 SELF.ip := SrcAddrIp,
                                 SELF.allowOverwrite := True,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    o2 := output(c2);
#else
    o2 := output(c2, {result});
#end


// This should fail based on 'localhost' used as source address
dst3 := NOFOLD(DATASET([{SourceFile, DestFile1, SrcAddrLocalhost, True, '', ''}], rec));

p3 := PROJECT(NOFOLD(dst3), t(LEFT));

c3 := CATCH(NOFOLD(p3), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile1,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := SrcAddrLocalhost,
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    o3 := output(c3);
#else
    o3 := output(c3, {result});
#end


// This should fail based on '/./' used in target path
DestFile4 := dropzonePath + './' + DestFileName;
dst4 := NOFOLD(DATASET([{SourceFile, DestFile4, SrcAddrIp, True, '', ''}], rec));

p4 := PROJECT(NOFOLD(dst4), t(LEFT));

c4 := CATCH(NOFOLD(p4), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile4,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := SrcAddrIp,
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    o4 := output(c4);
#else
    o4 := output(c4, {result});
#end


// This should fail based on '/../' used in target path
DestFile5 := dropzonePath + '../' + DestFileName;
dst5 := NOFOLD(DATASET([{SourceFile, DestFile5, SrcAddrIp, True, '', ''}], rec));

p5 := PROJECT(NOFOLD(dst5), t(LEFT));

c5 := CATCH(NOFOLD(p5), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile5,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := SrcAddrIp,
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    o5 := output(c5);
#else
    o5 := output(c5, {result});
#end


// This should fail based on
// not an existing dropzone path used in target file path
DestFile6 := '/var/lib/HPCCSystems/mydropzona/' + DestFileName;
dst6 := NOFOLD(DATASET([{SourceFile, DestFile6, SrcAddrIp, True, '', ''}], rec));

p6 := PROJECT(NOFOLD(dst6), t(LEFT));

c6 := CATCH(NOFOLD(p6), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile6,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := SrcAddrIp,
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    o6 := output(c6);
#else
    o6 := output(c6, {result});
#end


// This should fail based on
// try to despray out of a drop zone
DestFile7 := '/var/lib/HPCCSystems/' + File;
dst7 := NOFOLD(DATASET([{SourceFile, DestFile7, SrcAddrIp, True, '', ''}], rec));

p7 := PROJECT(NOFOLD(dst7), t(LEFT));

c7 := CATCH(NOFOLD(p7), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile7,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := SrcAddrIp,
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    o7 := output(c7);
#else
    o7 := output(c7, {result});
#end


// This should fail based on
// not an existing dropzone path used in target file path
DestFile8 := '/var/lib/HPCCSystems/mydropzone../' + File;
dst8 := NOFOLD(DATASET([{SourceFile, DestFile8, SrcAddrIp, True, '', ''}], rec));

p8 := PROJECT(NOFOLD(dst8), t(LEFT));

c8 := CATCH(NOFOLD(p8), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile8,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := SrcAddrIp,
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    o8 := output(c8);
#else
    o8 := output(c8, {result});
#end


// This should pass based on valid target file path and valid source address used
DestFile9 := dropzonePath + 'test/' + prefix + File;
dst9 := NOFOLD(DATASET([{SourceFile, DestFile9, SrcAddrIp, True, '', ''}], rec));

p9 := PROJECT(NOFOLD(dst9), t(LEFT));

c9 := CATCH(NOFOLD(p9), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile9,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := SrcAddrIp,
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    o9 := output(c9);
#else
    o9 := output(c9, {result});
#end


// Allow overwrite checking

// This shoud fail based on the previous despray already created a file on the target path
// and overwrite not allowed.
dst10 := NOFOLD(DATASET([{SourceFile, DestFile9, SrcAddrIp, False, '', ''}], rec));

p10 := PROJECT(NOFOLD(dst10), t(LEFT));

c10 := CATCH(NOFOLD(p10), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile9,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := SrcAddrIp,
                                 SELF.allowOverwrite := False,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    o10 := output(c10);
#else
    o10 := output(c10, {result});
#end

SEQUENTIAL(
  setupPeople,
  PARALLEL(
    o2,
    o3,
    o4,
    o5,
    o6,
    o7,
    o8,
    o9,
  ),
    // To ensure it running after o9
    o10,
    // Clean-up
    FileServices.DeleteLogicalFile(SourceFile),
    FileServices.DeleteExternalFile('.', DestFile1),
    //FileServices.DeleteExternalFile('.', DestFile4),
    //FileServices.DeleteExternalFile('.', DestFile5),
    //FileServices.DeleteExternalFile('.', DestFile6),
    //FileServices.DeleteExternalFile('.', DestFile7),
    //FileServices.DeleteExternalFile('.', DestFile8),
    FileServices.DeleteExternalFile('.', DestFile9),
);
