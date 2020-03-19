/*##############################################################################

    Copyright (C) 2017 HPCC Systems®.

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

//nohthor

//class=spray
//class=copy
//class=dfuplus

import Std.File AS FileServices;
import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;

dropzonePath := FileServices.GetDefaultDropZone() : STORED('dropzonePath');

unsigned VERBOSE := 0;

Layout_Person := RECORD
  STRING3  name;
  UNSIGNED2 age;
  BOOLEAN good;
END;

sprayPrepFileName := prefix + 'spray_prep';
desprayOutFileName := dropzonePath + WORKUNIT + '-spray_input';
sprayOutFileName := prefix + 'spray_test';

allPeople := DATASET([ {'foo', 10, 1},
                       {'bar', 12, 0},
                       {'baz', 32, 1}]
            ,Layout_Person);

//  Create a small logical file
setupCsv := output(allPeople, , sprayPrepFileName+'_CSV', CSV, OVERWRITE);
setupXml := output(allPeople, , sprayPrepFileName+'_XML', XML('Rowtag'), OVERWRITE);


rec := RECORD
  string result;
  string msg;
end;

// Despray it to default drop zone
rec despray(rec l) := TRANSFORM
  SELF.msg := FileServices.fDespray(
                       LOGICALNAME := sprayPrepFileName+'_CSV'
                      ,DESTINATIONIP := '.'
                      ,DESTINATIONPATH := desprayOutFileName+'_CSV'
                      ,ALLOWOVERWRITE := True
                      );
  SELF.result := 'Pass';
end;

dst1 := NOFOLD(DATASET([{'', ''}], rec));
p1 := NOTHOR(PROJECT(NOFOLD(dst1), despray(LEFT)));
c1 := CATCH(NOFOLD(p1), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Despray Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    desprayOutCsv := output(c1,,NAMED('desprayCsv'));
#else
    desprayOutCsv := output(c1, {result},NAMED('desprayCsv'));
#end

// Despray it to default drop zone
rec desprayXml(rec l) := TRANSFORM
  SELF.msg := FileServices.fDespray(
                       LOGICALNAME := sprayPrepFileName+'_XML'
                      ,DESTINATIONIP := '.'
                      ,DESTINATIONPATH := desprayOutFileName+'_XML'
                      ,ALLOWOVERWRITE := True
                      );
  SELF.result := 'Pass';
end;

dst2 := NOFOLD(DATASET([{'', ''}], rec));
p2 := NOTHOR(PROJECT(NOFOLD(dst1), desprayXml(LEFT)));
c2 := CATCH(NOFOLD(p2), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Despray Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    desprayOutXml := output(c2,,NAMED('desprayXml'));
#else
    desprayOutXml := output(c2, {result},NAMED('desprayXml'));
#end



ClusterName := 'mythor';
DestFile := prefix + 'spray_expire.txt';
ESPportIP := FileServices.GetEspURL() + '/FileSpray';

expireDaysOut1 := 9;
expireDaysIn1 := NOTHOR(FileServices.GetLogicalFileAttribute(DestFile, 'expireDays'));
res1:=if(expireDaysIn1 = intformat(expireDaysOut1,1,0), 'Pass', 'Fail ('+intformat(expireDaysOut1,1,0)+','+expireDaysIn1+')');

expireDaysOut2 := 7;
expireDaysIn2 := NOTHOR(FileServices.GetLogicalFileAttribute(DestFile, 'expireDays'));
res2:=if(expireDaysIn2 = intformat(expireDaysOut2,1,0), 'Pass', 'Fail ('+intformat(expireDaysOut2,1,0)+','+expireDaysIn2+')');

expireDaysOut3 := 11;
expireDaysIn3 := NOTHOR(FileServices.GetLogicalFileAttribute(DestFile, 'expireDays'));
res3:=if(expireDaysIn3 = intformat(expireDaysOut3,2,0), 'Pass', 'Fail ('+intformat(expireDaysOut3,2,0)+','+expireDaysIn3+')');

expireDaysOut4 := 13;
expireDaysIn4 := FileServices.GetLogicalFileAttribute(DestFile, 'expireDays');
res4:=if(expireDaysIn4 = intformat(expireDaysOut4,2,0), 'Pass', 'Fail ('+intformat(expireDaysOut4,2,0)+','+expireDaysIn4+')');


// DFUPlus tests

expireDaysOut5 := 17;
expireDaysIn5 := FileServices.GetLogicalFileAttribute(DestFile, 'expireDays');
res5:=if(expireDaysIn5 = intformat(expireDaysOut5,2,0), 'Pass', 'Fail ('+intformat(expireDaysOut5,2,0)+','+expireDaysIn5+')');

expireDaysOut6 := 0;
expireDaysIn6 := FileServices.GetLogicalFileAttribute(DestFile, 'expireDays');
res6:=if(expireDaysIn6 = intformat(expireDaysOut6,1,0), 'Pass', 'Fail ('+intformat(expireDaysOut6,2,0)+','+expireDaysIn6+')');

expireDaysOut7 := -1;
expireDaysIn7 := FileServices.GetLogicalFileAttribute(DestFile, 'expireDays');
res7:=if(expireDaysIn7 = '', 'Pass', 'Fail ('+intformat(expireDaysOut7,2,0)+','+expireDaysIn7+')');

expireDaysOut8 := -1; // but omit expireDay parameter
expireDaysIn8 := FileServices.GetLogicalFileAttribute(DestFile, 'expireDays');
res8:=if(expireDaysIn8 = '', 'Pass', 'Fail ('+intformat(expireDaysOut8,2,0)+','+expireDaysIn8+')');



CopyDestFile := prefix + 'copy_expire.txt';
expireDaysOut9 := 19;
expireDaysIn9 := FileServices.GetLogicalFileAttribute(CopyDestFile, 'expireDays');
res9:=if(expireDaysIn9 = intformat(expireDaysOut9,2,0), 'Pass', 'Fail ('+intformat(expireDaysOut9,2,0)+','+expireDaysIn9+')');

RemotePullDestFile := prefix + 'remote_pull_expire.txt';
expireDaysOut10 := 23;
expireDaysIn10 := FileServices.GetLogicalFileAttribute(RemotePullDestFile, 'expireDays');
res10:=if(expireDaysIn10 = intformat(expireDaysOut10,2,0), 'Pass', 'Fail ('+intformat(expireDaysOut10,2,0)+','+expireDaysIn10+')');


sequential (
    //output(dropzonePath, NAMED('dropzonePath')),
    // Preparation
    setupCsv,
    setupXml,
    desprayOutCsv,
    desprayOutXml,

    // Spray tests
    FileServices.SprayVariable(
                        sourceIP := '.',
                        sourcePath := desprayOutFileName+'_CSV',
                        sourceMaxRecordSize :=8192,
                        sourceCsvSeparate :='\\,',
                        sourceCsvTerminate :='\\n,\\r\\n',
                        sourceCsvQuote :='\"',
                        destinationGroup := ClusterName,
                        destinationLogicalName := DestFile,
                        timeOut :=-1,
                        espServerIpPort := ESPportIP,
                        maxConnections :=-1,
                        allowOverwrite :=TRUE,
                        replicate :=FALSE,
                        compress :=FALSE,
                        sourceCsvEscape :='',
                        failIfNoSourceFile :=FALSE,
                        recordStructurePresent :=FALSE,
                        quotedTerminator :=TRUE,
                        encoding :='ascii',
                        expireDays :=expireDaysOut1
                        ),
    output(res1,NAMED('SprayVariable')),
    FileServices.SprayDelimited(
                          sourceIP := '.',
                          sourcePath := desprayOutFileName+'_CSV',
                          sourceMaxRecordSize :=8192,
                          sourceCsvSeparate :='\\,',
                          sourceCsvTerminate :='\\n,\\r\\n',
                          sourceCsvQuote :='\"',
                          destinationGroup := ClusterName,
                          destinationLogicalName := DestFile,
                          timeOut :=-1,
                          espServerIpPort := ESPportIP,
                          maxConnections :=-1,
                          allowOverwrite :=TRUE,
                          replicate :=FALSE,
                          compress :=FALSE,
                          sourceCsvEscape :='',
                          failIfNoSourceFile :=FALSE,
                          recordStructurePresent :=FALSE,
                          quotedTerminator :=TRUE,
                          encoding :='ascii',
                          expireDays :=expireDaysOut2
                        ),
    output(res2,NAMED('SprayDelimited')),
    FileServices.SprayFixed(
                            SOURCEIP := '.',
                            SOURCEPATH := desprayOutFileName+'_CSV',
                            RECORDSIZE := 9,
                            DESTINATIONGROUP := ClusterName,
                            DESTINATIONLOGICALNAME := DestFile,
                            TIMEOUT := -1,
                            ESPSERVERIPPORT := ESPportIP,
                            ALLOWOVERWRITE := true,
                            expireDays :=expireDaysOut3
                            );
    output(res3,NAMED('SprayFixed')),
    FileServices.SprayXml(
                            SOURCEIP := '.',
                            SOURCEPATH := desprayOutFileName+'_XML',
                            SOURCEROWTAG := 'Rowtag',
                            DESTINATIONGROUP := 'mythor',
                            DESTINATIONLOGICALNAME := DestFile,
                            TIMEOUT := -1,
                            ESPSERVERIPPORT := ESPportIP,
                            ALLOWOVERWRITE := true,
                            expireDays :=expireDaysOut4
                            );
    output(res4,NAMED('SprayXml')),

    FileServices.DfuPlusExec('action=spray srcip=. srcfile='+desprayOutFileName+'_CSV dstname='+DestFile+' jobname=spray_expire_csv server=. dstcluster=mythor format=csv overwrite=1 replicate=0 expireDays='+intformat(expireDaysOut5,2,0)),
    output(res5,NAMED('DFUPlus')),

    FileServices.DfuPlusExec('action=spray srcip=. srcfile='+desprayOutFileName+'_CSV dstname='+DestFile+' jobname=spray_expire_csv server=. dstcluster=mythor format=csv overwrite=1 replicate=0 expireDays='+intformat(expireDaysOut6,2,0)),
    output(res6,NAMED('DFUPlus2')),

    FileServices.DfuPlusExec('action=spray srcip=. srcfile='+desprayOutFileName+'_CSV dstname='+DestFile+' jobname=spray_expire_csv server=. dstcluster=mythor format=csv overwrite=1 replicate=0 expireDays='+intformat(expireDaysOut7,2,0)),
    output(res7,NAMED('DFUPlus3')),

    FileServices.DfuPlusExec('action=spray srcip=. srcfile='+desprayOutFileName+'_CSV dstname='+DestFile+' jobname=spray_expire_csv server=. dstcluster=mythor format=csv overwrite=1 replicate=0'),
    output(res8,NAMED('DFUPlus4')),


    FileServices.Copy(
                            sourceLogicalName := sprayPrepFileName+'_CSV',
                            destinationGroup := ClusterName,
                            destinationLogicalName := CopyDestFile,
                            sourceDali := '.',
                            timeOut := -1,
                            espServerIpPort := ESPportIP,
                            ALLOWOVERWRITE := true,
                            expireDays := expireDaysOut9
                            );
    output(res9,NAMED('Copy')),

    FileServices.RemotePull(
                             remoteEspFsURL := ESPportIP,
                             sourceLogicalName := sprayPrepFileName+'_CSV',
                             destinationGroup := ClusterName,
                             destinationLogicalName := RemotePullDestFile,
                             TIMEOUT := -1,
                             ALLOWOVERWRITE := true,
                             expireDays := expireDaysOut10
                             );
     output(res10, NAMED('RemotePull')),

    // Clean-up
    FileServices.DeleteExternalFile('.', desprayOutFileName+'_CSV'),
    FileServices.DeleteExternalFile('.', desprayOutFileName+'_XML'),

    FileServices.DeleteLogicalFile(DestFile),
    FileServices.DeleteLogicalFile(sprayPrepFileName+'_CSV'),
    FileServices.DeleteLogicalFile(sprayPrepFileName+'_XML'),
    FileServices.DeleteLogicalFile(CopyDestFile),
    FileServices.DeleteLogicalFile(RemotePullDestFile),
);

