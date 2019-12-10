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
//noroxie

//class=spray
//class=copy
//class=dfuplus

import Std.File AS FileServices;
import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;

dropzonePath := '/var/lib/HPCCSystems/mydropzone/' : STORED('dropzonePath');

unsigned VERBOSE := 0;
unsigned CLEAN_UP := 0;

Layout_Person := RECORD
  STRING5  name;
  String2 age;
  BOOLEAN good;
END;

// This need for sprayFixed()
recSize := Sizeof(Layout_Person) + 3; // 3 = 2 field separator + terminator

origRecCount := 10000; // count(allPeople);

allPeople := DATASET( origRecCount,
                      TRANSFORM(layout_Person,
                                        SELF.name := INTFORMAT(COUNTER, sizeof(Layout_Person.name), 1);
                                        SELF.age  := INTFORMAT((COUNTER%99), sizeof(Layout_Person.age), 1);
                                        SELF.good := IF(COUNTER%2=1, TRUE, FALSE)
                               ),
                      DISTRIBUTED);


//  Create logical files

sprayPrepFileName := prefix + 'spray_prep';
setupCsv := output(allPeople, , sprayPrepFileName+'_CSV', CSV, OVERWRITE);
setupXml := output(allPeople, , sprayPrepFileName+'_XML', XML('Rowtag'), OVERWRITE);


desprayOutFileName := dropzonePath + WORKUNIT + '-spray_input';

desprayRec := RECORD
  string suffix;
  string result;
  string msg;
end;

// Despray it to default drop zone
desprayRec despray(desprayRec l) := TRANSFORM
  SELF.msg := FileServices.fDespray(
                       LOGICALNAME := sprayPrepFileName + l.suffix
                      ,DESTINATIONIP := '.'
                      ,DESTINATIONPATH := desprayOutFileName + l.suffix
                      ,ALLOWOVERWRITE := True
                      );
  SELF.result := 'Pass';
  SELF.suffix := l.suffix;
end;

dst1 := NOFOLD(DATASET([{'_CSV', '', ''}], desprayRec));
p1 := NOTHOR(PROJECT(NOFOLD(dst1), despray(LEFT)));
c1 := CATCH(NOFOLD(p1), ONFAIL(TRANSFORM(desprayRec,
                                 SELF.result := 'Despray Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.suffix := '_CSV'
                                )));

#if (VERBOSE = 1)
    desprayOutCsv := output(c1,,NAMED('desprayCsv'));
#else
    desprayOutCsv := output(c1, {result},NAMED('desprayCsv'));
#end

dst2 := NOFOLD(DATASET([{'_XML', '', ''}], desprayRec));
p2 := NOTHOR(PROJECT(NOFOLD(dst2), despray(LEFT)));
c2 := CATCH(NOFOLD(p2), ONFAIL(TRANSFORM(desprayRec,
                                 SELF.result := 'Despray Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.suffix := '_XML'
                                )));

#if (VERBOSE = 1)
    desprayOutXml := output(c2,,NAMED('desprayXml'));
#else
    desprayOutXml := output(c2, {result},NAMED('desprayXml'));
#end



SprayClusterName := 'mythor';
CopySplitClusterName := 'myroxie';
CopyNoSplitClusterName := 'mythor';

DestFile := 'split_test';
ESPportIP := 'http://127.0.0.1:8010/FileSpray';

gatherCounts(string filename, unsigned origRecCount, boolean noSplit, string operation = 'variable') := FUNCTION

    r := RECORD
        cnt := COUNT(GROUP);
    end;

    // Check recordcount and first part record count
    ds :=  if (operation = 'xml',
                dataset(filename, Layout_Person, XML('Dataset/Rowtag')),
                dataset(filename, Layout_Person, CSV)
              );
    tbl := TABLE(ds, r, local);
    newRecCount := sum(tbl,cnt);

    res1:=if(origRecCount = newRecCount, 'Pass', 'Fail ('+intformat(origRecCount,1,0)+','+intformat(newRecCount,1,0)+')');
    res2:=if(noSplit, if(origRecCount = tbl[1].cnt, 'Pass', 'Fail ('+intformat(origRecCount,1,0)+','+intformat(tbl[1].cnt,1,0)+')'), 'Pass');

    RETURN DATASET(ROW(TRANSFORM({ STRING recordCountCheck, STRING firstPartRecordCountCheck }, self.recordCountCheck := res1; self.firstPartRecordCountCheck := res2) ));
END;


sprayRec := RECORD
  string  operation;
  string sourceFilename;
  string targetFilename;
  string targetCluster;
  boolean noSplit;
  string result;
  string msg;
end;

// Variable spray tests

sprayRec spray(sprayRec l) := TRANSFORM
     SELF.msg :=
     if ( l.operation = 'variable',
            FileServices.fSprayVariable(
                            SOURCEIP := '.',
                            SOURCEPATH := l.sourceFilename,
                            DESTINATIONGROUP := l.targetCluster,
                            DESTINATIONLOGICALNAME := l.targetFilename,
                            TIMEOUT := -1,
                            ESPSERVERIPPORT := 'http://127.0.0.1:8010/FileSpray',
                            ALLOWOVERWRITE := true,
                            NOSPLIT :=  l.noSplit
                            ),

        if ( l.operation = 'fixed',
             FileServices.fSprayFixed(
                                SOURCEIP := '.',
                                SOURCEPATH :=  l.sourceFilename,
                                RECORDSIZE := recSize,
                                DESTINATIONGROUP := l.targetCluster,
                                DESTINATIONLOGICALNAME := l.targetFilename,
                                TIMEOUT := -1,
                                ESPSERVERIPPORT := 'http://127.0.0.1:8010/FileSpray',
                                ALLOWOVERWRITE := true,
                                NOSPLIT :=  l.noSplit
                                ),
             if ( l.operation = 'delimited',
                FileServices.fSprayDelimited(
                          SOURCEIP := '.',
                          SOURCEPATH :=  l.sourceFilename,
                          DESTINATIONGROUP := l.targetCluster,
                          DESTINATIONLOGICALNAME := l.targetFilename,
                          TIMEOUT := -1,
                          ESPSERVERIPPORT := 'http://127.0.0.1:8010/FileSpray',
                          ALLOWOVERWRITE := true,
                          NOSPLIT :=  l.noSplit
                        ),
                    if ( l.operation = 'xml',
                        FileServices.fSprayXml(
                            SOURCEIP := '.',
                            SOURCEPATH := l.sourceFilename,
                            SOURCEROWTAG := 'Rowtag',
                            DESTINATIONGROUP :=  l.targetCluster,
                            DESTINATIONLOGICALNAME := l.targetFilename,
                            TIMEOUT := -1,
                            ESPSERVERIPPORT := 'http://127.0.0.1:8010/FileSpray',
                            ALLOWOVERWRITE := true,
                            NOSPLIT :=  l.noSplit
                            ),
                        if (l.operation = 'copy',
                                FileServices.fCopy(
                                    sourceLogicalName := l.sourceFilename,
                                    destinationGroup := l.targetCluster, //'myroxie', //ClusterName,
                                    destinationLogicalName := l.targetFilename,
                                    sourceDali := '.',
                                    TIMEOUT := -1,
                                    ESPSERVERIPPORT := 'http://127.0.0.1:8010/FileSpray',
                                    ALLOWOVERWRITE := true,
                                    NOSPLIT :=  l.noSplit
                                    ),
                                if (l.operation = 'remotePull',
                                    FileServices.fRemotePull(
                                         remoteEspFsURL := 'http://127.0.0.1:8010/FileSpray',
                                         sourceLogicalName := l.sourceFilename,
                                         destinationGroup := l.targetCluster,
                                         destinationLogicalName := l.targetFilename,
                                         TIMEOUT := -1,
                                         ALLOWOVERWRITE := true,
                                         NOSPLIT :=  l.noSplit
                                     ),
                                     'Wrong operation: "' + l.operation +'".'
                               )
                        )
                    )
                )
          )
       );

    self.operation := l.operation;
    self.noSplit := l.noSplit;
    self.sourceFilename := l.sourceFilename;
    self.targetFilename := l.targetFilename;
    self.targetCluster := l.targetCluster;
    self.result := 'Pass';
end;

dst3 := NOFOLD(DATASET([{'variable', desprayOutFileName + '_CSV', prefix + DestFile + '_variable_CSV', SprayClusterName, false, '', ''}], sprayRec));
p3 := NOTHOR(PROJECT(NOFOLD(dst3), spray(LEFT)));
c3 := CATCH(NOFOLD(p3), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.noSplit := dst3[1].noSplit,
                                 SELF.operation := dst3[1].operation,
                                 SELF.sourceFilename := dst3[1].sourceFilename,
                                 SELF.targetFilename := dst3[1].targetFilename,
                                 SELF.targetCluster :=  dst3[1].targetCluster
                                )));
#if (VERBOSE = 1)
    sprayVariableSplit := output(c3, named('sprayVariableSplitCheck'));
#else
    sprayVariableSplit := output(c3, {result}, named('sprayVariableSplitCheck'));
#end

// Check recordcount
res3 := gatherCounts(dst3[1].targetFilename, origRecCount, dst3[1].noSplit);



dst4 := NOFOLD(DATASET([{'variable', desprayOutFileName + '_CSV', prefix + 'no' + DestFile + '_variable_CSV', SprayClusterName, true, '', ''}], sprayRec));
p4 := NOTHOR(PROJECT(NOFOLD(dst4), spray(LEFT)));
c4 := CATCH(NOFOLD(p4), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.noSplit := dst4[1].noSplit,
                                 SELF.operation := dst4[1].operation,
                                 SELF.sourceFilename := dst4[1].sourceFilename,
                                 SELF.targetFilename := dst4[1].targetFilename,
                                 SELF.targetCluster :=  dst4[1].targetCluster
                                )));
#if (VERBOSE = 1)
    sprayVariableNoSplit := output(c4, named('sprayVariableNoSplitCheck'));
#else
    sprayVariableNoSplit := output(c4, {result}, named('sprayVariableNoSplitCheck'));
#end

// Check recordcount and first part record count
res4 := gatherCounts(dst4[1].targetFilename, origRecCount, dst4[1].noSplit);


// Fixed spray tests

dst5 := NOFOLD(DATASET([{'fixed', desprayOutFileName + '_CSV', prefix + DestFile + '_fixed_CSV', SprayClusterName, false, '', ''}], sprayRec));
p5 := NOTHOR(PROJECT(NOFOLD(dst5), spray(LEFT)));
c5 := CATCH(NOFOLD(p5), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.noSplit := dst5[1].noSplit,
                                 SELF.operation := dst5[1].operation,
                                 SELF.sourceFilename := dst5[1].sourceFilename,
                                 SELF.targetFilename := dst5[1].targetFilename,
                                 SELF.targetCluster :=  dst5[1].targetCluster
                                )));
#if (VERBOSE = 1)
    sprayFixedSplit := output(c5, named('sprayFixedSplitCheck'));
#else
    sprayFixedSplit := output(c5, {result}, named('sprayFixedSplitCheck'));
#end

// Check recordcount
res5 := gatherCounts(dst5[1].targetFilename, origRecCount, dst5[1].noSplit);



dst6 := NOFOLD(DATASET([{'fixed', desprayOutFileName + '_CSV', prefix + 'no' + DestFile + '_fixed_CSV', SprayClusterName, true, '', ''}], sprayRec));
p6 := NOTHOR(PROJECT(NOFOLD(dst6), spray(LEFT)));
c6 := CATCH(NOFOLD(p6), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.noSplit := dst6[1].noSplit,
                                 SELF.operation := dst6[1].operation,
                                 SELF.sourceFilename := dst6[1].sourceFilename,
                                 SELF.targetFilename := dst6[1].targetFilename,
                                 SELF.targetCluster :=  dst6[1].targetCluster
                                )));
#if (VERBOSE = 1)
    sprayFixedNoSplit := output(c6, named('sprayFixedNoSplitCheck'));
#else
    sprayFixedNoSplit := output(c6, {result}, named('sprayFixedNoSplitCheck'));
#end

// Check recordcount and first part record count
res6 := gatherCounts(dst6[1].targetFilename, origRecCount, dst6[1].noSplit);



// Delimmited spray tests

dst7 := NOFOLD(DATASET([{'delimited', desprayOutFileName + '_CSV', prefix + DestFile + '_delimited_CSV', SprayClusterName, false, '', ''}], sprayRec));
p7 := NOTHOR(PROJECT(NOFOLD(dst7), spray(LEFT)));
c7 := CATCH(NOFOLD(p7), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.noSplit := dst7[1].noSplit,
                                 SELF.operation := dst7[1].operation,
                                 SELF.sourceFilename := dst7[1].sourceFilename,
                                 SELF.targetFilename := dst7[1].targetFilename,
                                 SELF.targetCluster :=  dst7[1].targetCluster
                                )));
#if (VERBOSE = 1)
    sprayDelimitedSplit := output(c7, named('sprayDelimitedSplitCheck'));
#else
    sprayDelimitedSplit := output(c7, {result}, named('sprayDelimitedSplitCheck'));
#end

// Check recordcount
res7 := gatherCounts(dst7[1].targetFilename, origRecCount, dst7[1].noSplit);



dst8 := NOFOLD(DATASET([{'delimited', desprayOutFileName + '_CSV', prefix + 'no' + DestFile + '_delimited_CSV', SprayClusterName, true, '', ''}], sprayRec));
p8 := NOTHOR(PROJECT(NOFOLD(dst8), spray(LEFT)));
c8 := CATCH(NOFOLD(p8), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.noSplit := dst8[1].noSplit,
                                 SELF.operation := dst8[1].operation,
                                 SELF.sourceFilename := dst8[1].sourceFilename,
                                 SELF.targetFilename := dst8[1].targetFilename,
                                 SELF.targetCluster :=  dst8[1].targetCluster
                                )));
#if (VERBOSE = 1)
    sprayDelimitedNoSplit := output(c8, named('sprayDelimitedNoSplitCheck'));
#else
    sprayDelimitedNoSplit := output(c8, {result}, named('sprayDelimitedNoSplitCheck'));
#end

// Check recordcount and first part record count
res8 := gatherCounts(dst8[1].targetFilename, origRecCount, dst8[1].noSplit);


// XML spray tests

dst9 := NOFOLD(DATASET([{'xml', desprayOutFileName + '_XML', prefix + DestFile + '_XML', SprayClusterName, false, '', ''}], sprayRec));
p9 := NOTHOR(PROJECT(NOFOLD(dst9), spray(LEFT)));
c9 := CATCH(NOFOLD(p9), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.noSplit := dst9[1].noSplit,
                                 SELF.operation := dst9[1].operation,
                                 SELF.sourceFilename := dst9[1].sourceFilename,
                                 SELF.targetFilename := dst9[1].targetFilename,
                                 SELF.targetCluster :=  dst9[1].targetCluster
                                )));
#if (VERBOSE = 1)
    sprayXmlSplit := output(c9, named('ssprayXmlSplitCheck'));
#else
    sprayXmlSplit := output(c9, {result}, named('sprayXmlSplitCheck'));
#end

// Check recordcount
res9 := gatherCounts(dst9[1].targetFilename, origRecCount, dst9[1].noSplit, 'xml');



dst10 := NOFOLD(DATASET([{'xml', desprayOutFileName + '_XML', prefix + 'no' + DestFile + '_XML', SprayClusterName, true, '', ''}], sprayRec));
p10 := NOTHOR(PROJECT(NOFOLD(dst10), spray(LEFT)));
c10 := CATCH(NOFOLD(p10), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.noSplit := dst10[1].noSplit,
                                 SELF.operation := dst10[1].operation,
                                 SELF.sourceFilename := dst10[1].sourceFilename,
                                 SELF.targetFilename := dst10[1].targetFilename,
                                 SELF.targetCluster :=  dst10[1].targetCluster
                                )));
#if (VERBOSE = 1)
    sprayXmlNoSplit := output(c10, named('sprayXmlNoSplitCheck'));
#else
    sprayXmlNoSplit := output(c10, {result}, named('sprayXmlNoSplitCheck'));
#end

// Check recordcount and first part record count
res10 := gatherCounts(dst10[1].targetFilename, origRecCount, dst10[1].noSplit, 'xml');


// Copy tests

dst11 := NOFOLD(DATASET([{'copy', prefix + DestFile + '_variable_CSV', prefix + DestFile +'_copy_CSV', CopySplitClusterName, false, '', ''}], sprayRec));
p11 := NOTHOR(PROJECT(NOFOLD(dst11), spray(LEFT)));
c11 := CATCH(NOFOLD(p11), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.noSplit := dst11[1].noSplit,
                                 SELF.operation := dst11[1].operation,
                                 SELF.sourceFilename := dst11[1].sourceFilename,
                                 SELF.targetFilename := dst11[1].targetFilename,
                                 SELF.targetCluster :=  dst11[1].targetCluster
                                )));
#if (VERBOSE = 1)
    copySplit := output(c11, named('copySplitCheck'));
#else
    copySplit := output(c11, {result}, named('copySplitCheck'));
#end

// Check recordcount
res11 := gatherCounts(dst11[1].targetFilename, origRecCount, dst11[1].noSplit);



dst12 := NOFOLD(DATASET([{'copy', prefix + DestFile + '_copy_CSV', prefix + 'no' + DestFile + '_copy_CSV', CopyNoSplitClusterName, true, '', ''}], sprayRec));
p12 := NOTHOR(PROJECT(NOFOLD(dst12), spray(LEFT)));
c12 := CATCH(NOFOLD(p12), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.noSplit := dst12[1].noSplit,
                                 SELF.operation := dst12[1].operation,
                                 SELF.sourceFilename := dst12[1].sourceFilename,
                                 SELF.targetFilename := dst12[1].targetFilename,
                                 SELF.targetCluster :=  dst12[1].targetCluster
                                )));
#if (VERBOSE = 1)
    copyNoSplit := output(c12, named('copyNoSplitCheck'));
#else
    copyNoSplit := output(c12, {result}, named('copyNoSplitCheck'));
#end

// Check recordcount and first part record count
res12 := gatherCounts(dst12[1].targetFilename, origRecCount, dst12[1].noSplit);




// RemotePull test

dst13 := NOFOLD(DATASET([{'remotePull', prefix + DestFile + '_copy_CSV', prefix + DestFile +'_remotePull_CSV', CopySplitClusterName, false, '', ''}], sprayRec));
p13 := NOTHOR(PROJECT(NOFOLD(dst13), spray(LEFT)));
c13 := CATCH(NOFOLD(p13), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.noSplit := dst13[1].noSplit,
                                 SELF.operation := dst13[1].operation,
                                 SELF.sourceFilename := dst13[1].sourceFilename,
                                 SELF.targetFilename := dst13[1].targetFilename,
                                 SELF.targetCluster :=  dst13[1].targetCluster
                                )));
#if (VERBOSE = 1)
    remotePullSplit := output(c13, named('remotePullSplitCheck'));
#else
    remotePullSplit := output(c13, {result}, named('remotePullSplitCheck'));
#end

// Check recordcount
res13 := gatherCounts(dst13[1].targetFilename, origRecCount, dst13[1].noSplit);



dst14 := NOFOLD(DATASET([{'remotePull', prefix + DestFile + '_copy_CSV', prefix + 'no' + DestFile + '_remotePull_CSV', CopyNoSplitClusterName, true, '', ''}], sprayRec));
p14 := NOTHOR(PROJECT(NOFOLD(dst14), spray(LEFT)));
c14 := CATCH(NOFOLD(p14), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.result := 'Fail',
                                 SELF.msg := FAILMESSAGE,
                                 SELF.noSplit := dst14[1].noSplit,
                                 SELF.operation := dst14[1].operation,
                                 SELF.sourceFilename := dst14[1].sourceFilename,
                                 SELF.targetFilename := dst14[1].targetFilename,
                                 SELF.targetCluster :=  dst14[1].targetCluster
                                )));
#if (VERBOSE = 1)
    remotePullNoSplit := output(c14, named('remotePullNoSplitNoSplitCheck'));
#else
    remotePullNoSplit := output(c14, {result}, named('remotePullNoSplitNoSplitCheck'));
#end

// Check recordcount and first part record count
res14 := gatherCounts(dst14[1].targetFilename, origRecCount, dst14[1].noSplit);




// Let's do it

sequential (
#if (VERBOSE = 1)
    output(dropzonePath, NAMED('dropzonePath')),
#end

    // Preparation
    setupCsv,
    setupXml,
    desprayOutCsv,
    desprayOutXml,


    // Spray tests
    sprayVariableSplit,
    output(res3, NAMED('sprayVariableSplitRecCountCheck')),

    sprayVariableNoSplit,
    output(res4,NAMED('sprayVariableNoSplitRecCountCheck')),


    sprayFixedSplit,
    output(res5, NAMED('sprayFixedSplitRecCountCheck')),

    sprayFixedNoSplit,
    output(res6, NAMED('sprayFixedNoSplitRecCountCheck')),


    sprayDelimitedSplit,
    output(res7, NAMED('sprayDelimitedSplitRecCountCheck')),

    sprayDelimitedNoSplit,
    output(res8, NAMED('sprayDelimitedNoSplitRecCountCheck')),


    sprayXmlSplit,
    output(res9, NAMED('sprayXmlSplitRecCountCheck')),

    sprayXmlNoSplit,
    output(res10, NAMED('sprayXmlNoSplitRecCountCheck')),


    copySplit,
    output(res11, NAMED('copySplitRecCountCheck')),

    copyNoSplit,
    output(res12, NAMED('copyNoSplitRecCountCheck')),


    remotePullSplit,
    output(res13, NAMED('remotePullSplitRecCountCheck')),

    remotePullNoSplit,
    output(res14, NAMED('remotePullNoSplitRecCountCheck')),


    #if (CLEAN_UP = 1)
    // Clean-up
    FileServices.DeleteExternalFile('.', desprayOutFileName+'_CSV'),
    FileServices.DeleteExternalFile('.', desprayOutFileName+'_XML'),

    FileServices.DeleteLogicalFile(prefix + DestFile + '_variable_CSV'),
    FileServices.DeleteLogicalFile(prefix + 'no' + DestFile + '_variable_CSV'),

    FileServices.DeleteLogicalFile(prefix + DestFile+'_fixed_CSV'),
    FileServices.DeleteLogicalFile(prefix + 'no' + DestFile+'_fixed_CSV'),

    FileServices.DeleteLogicalFile(prefix + DestFile+'_delimited_CSV'),
    FileServices.DeleteLogicalFile(prefix + 'no' + DestFile+'_delimited_CSV'),

    FileServices.DeleteLogicalFile(prefix + DestFile+'_XML'),
    FileServices.DeleteLogicalFile(prefix + 'no' + DestFile+'_XML'),

    FileServices.DeleteLogicalFile(prefix + DestFile+'_copy_CSV'),
    FileServices.DeleteLogicalFile(prefix + 'no' + DestFile+'_copy_CSV'),

    FileServices.DeleteLogicalFile(prefix + DestFile+'_remotePull_CSV'),
    FileServices.DeleteLogicalFile(prefix + 'no' + DestFile+'_remotePull_CSV'),

    FileServices.DeleteLogicalFile(sprayPrepFileName+'_CSV'),
    FileServices.DeleteLogicalFile(sprayPrepFileName+'_XML'),
    #end
);
