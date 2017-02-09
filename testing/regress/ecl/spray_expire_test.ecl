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

import Std.File AS FileServices;

// This is not an engine test, but a DFU.
// Doesn't matter much which engine does it, so we restrict to only one

//noRoxie
//noThorLCR
//noThor


unsigned VERBOSE := 0;

Layout_Person := RECORD
  STRING3  name;
  UNSIGNED2 age;
  BOOLEAN good;
END;

sprayPrepFileName := '~REGRESS::spray_prep';
desprayOutFileName := '/var/lib/HPCCSystems/mydropzone/spray_input';
sprayOutFileName := '~REGRESS::spray_test';

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
p1 := PROJECT(NOFOLD(dst1), despray(LEFT));
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
p2 := PROJECT(NOFOLD(dst1), desprayXml(LEFT));
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
DestFile := '~regress::spray_expire.txt';
ESPportIP := 'http://127.0.0.1:8010/FileSpray';

expireDaysOut1 := 9;
expireDaysIn1 := FileServices.GetLogicalFileAttribute(DestFile, 'expireDays');
res1:=if(expireDaysIn1 = intformat(expireDaysOut1,1,0), 'Pass', 'Fail ('+intformat(expireDaysOut1,1,0)+','+expireDaysIn1+')');

expireDaysOut2 := 7;
expireDaysIn2 := FileServices.GetLogicalFileAttribute(DestFile, 'expireDays');
res2:=if(expireDaysIn2 = intformat(expireDaysOut2,1,0), 'Pass', 'Fail ('+intformat(expireDaysOut2,1,0)+','+expireDaysIn2+')');

expireDaysOut3 := 11;
expireDaysIn3 := FileServices.GetLogicalFileAttribute(DestFile, 'expireDays');
res3:=if(expireDaysIn3 = intformat(expireDaysOut3,2,0), 'Pass', 'Fail ('+intformat(expireDaysOut3,2,0)+','+expireDaysIn3+')');

expireDaysOut4 := 13;
expireDaysIn4 := FileServices.GetLogicalFileAttribute(DestFile, 'expireDays');
res4:=if(expireDaysIn4 = intformat(expireDaysOut4,2,0), 'Pass', 'Fail ('+intformat(expireDaysOut4,2,0)+','+expireDaysIn4+')');

expireDaysOut5 := 17;
expireDaysIn5 := FileServices.GetLogicalFileAttribute(DestFile, 'expireDays');
res5:=if(expireDaysIn5 = intformat(expireDaysOut5,2,0), 'Pass', 'Fail ('+intformat(expireDaysOut5,2,0)+','+expireDaysIn5+')');


sequential (
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
                            DESTINATIONGROUP := 'mythor',
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


    // Clean-up
    FileServices.DeleteExternalFile('.', desprayOutFileName+'_CSV'),
    FileServices.DeleteExternalFile('.', desprayOutFileName+'_XML'),
    FileServices.DeleteLogicalFile(DestFile),
    FileServices.DeleteLogicalFile(sprayPrepFileName+'_CSV'),
    FileServices.DeleteLogicalFile(sprayPrepFileName+'_XML'),
);
