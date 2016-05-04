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
############################################################################## */

//nothor

import Std.File AS FileServices;

unsigned VERBOSE := 0;

Layout_Person := RECORD
  UNSIGNED1 PersonID;
  STRING15  FirstName;
  STRING25  LastName;
END;

allPeople := DATASET([ {1,'Fred','Smith'},
                       {2,'Joe','Blow'},
                       {3,'Jane','Smith'}],Layout_Person);

//  Outputs  ---
output(allPeople, , '~persons', OVERWRITE);

import * from lib_fileservices;

SrcAddrIp := '.';
SrcAddrLocalhost := 'localhost';
File := 'persons';
SourceFile := '~::' + File;

ClusterName := 'mythor';
DestFile1 := '/var/lib/HPCCSystems/mydropzone/' + File;

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

// Target path validation chekcing

dst0 := NOFOLD(DATASET([{SourceFile, DestFile1, SrcAddrIp, True, '', ''}], rec));
p0 := PROJECT(NOFOLD(dst0), t(LEFT));
c0 := CATCH(p0, ONFAIL(TRANSFORM(rec,
                                 SELF.sourceFile := SourceFile,
                                 SELF.destFile := DestFile1,
                                 SELF.ip := '',
                                 SELF.allowOverwrite := True,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    output(c0);
#else
    output(c0, {result});
#end


dst1 := NOFOLD(DATASET([{SourceFile, DestFile1, SrcAddrLocalhost, True, '', ''}], rec));
p1 := PROJECT(NOFOLD(dst1), t(LEFT));
c1 := CATCH(p1, ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile1,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := '',
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    output(c1);
#else
    output(c1, {result});
#end


DestFile2 := '/var/lib/HPCCSystems/mydropzone/./' + File;
dst2 := NOFOLD(DATASET([{SourceFile, DestFile2, SrcAddrIp, True, '', ''}], rec));
p2 := PROJECT(NOFOLD(dst2), t(LEFT));
c2 := CATCH(p2, ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile1,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := '',
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    output(c2);
#else
    output(c2, {result});
#end

DestFile3 := '/var/lib/HPCCSystems/mydropzone/../' + File;
dst3 := NOFOLD(DATASET([{SourceFile, DestFile3, SrcAddrIp, True, '', ''}], rec));
p3 := PROJECT(NOFOLD(dst3), t(LEFT));
c3 := CATCH(p3, ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile1,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := '',
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    output(c3);
#else
    output(c3, {result});
#end

DestFile4 := '/var/lib/HPCCSystems/mydropzona/' + File;
dst4 := NOFOLD(DATASET([{SourceFile, DestFile4, SrcAddrIp, True, '', ''}], rec));
p4 := PROJECT(NOFOLD(dst4), t(LEFT));
c4 := CATCH(p4, ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile1,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := '',
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    output(c4);
#else
    output(c4, {result});
#end

DestFile5 := '/var/lib/HPCCSystems/' + File;
dst5 := NOFOLD(DATASET([{SourceFile, DestFile5, SrcAddrIp, True, '', ''}], rec));
p5 := PROJECT(NOFOLD(dst5), t(LEFT));
c5 := CATCH(p5, ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile1,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := '',
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    output(c5);
#else
    output(c5, {result});
#end

DestFile6 := '/var/lib/HPCCSystems/mydropzone../' + File;
dst6 := NOFOLD(DATASET([{SourceFile, DestFile6, SrcAddrIp, True, '', ''}], rec));
p6 := PROJECT(NOFOLD(dst6), t(LEFT));
c6 := CATCH(p6, ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile1,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := '',
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    output(c6);
#else
    output(c6, {result});
#end

DestFile7 := '/var/lib/HPCCSystems/mydropzone/test/' + File;
dst7 := NOFOLD(DATASET([{SourceFile, DestFile7, SrcAddrIp, True, '', ''}], rec));
p7 := PROJECT(NOFOLD(dst7), t(LEFT));
c7 := CATCH(p7, ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile1,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := '',
                                 SELF.allowOverwrite := True,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    output(c7);
#else
    output(c7, {result});
#end

// Allow overwrite chekcing

dst8 := NOFOLD(DATASET([{SourceFile, DestFile7, SrcAddrIp, False, '', ''}], rec));
p8 := PROJECT(NOFOLD(dst8), t(LEFT));
c8 := CATCH(p8, ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Fail',
                                 SELF.destFile := DestFile1,
                                 SELF.sourceFile := SourceFile,
                                 SELF.ip := '',
                                 SELF.allowOverwrite := False,
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    output(c8);
#else
    output(c8, {result});
#end
