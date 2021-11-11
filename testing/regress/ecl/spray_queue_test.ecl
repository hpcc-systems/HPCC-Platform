/*##############################################################################

    Copyright (C) 2019 HPCC Systems®.

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
//class=spray

import std.system.thorlib;
import Std.File AS FileServices;
import $.setup;

dropzonePath := ''; // Use relative paths
prefix := setup.Files(false, false).QueryFilePrefix;
#if (__CONTAINERIZED__)
defaultDfuQueueName := 'dfuserver.dfuserver';
sprayDestGroup := 'data';
#else
defaultDfuQueueName := 'dfuserver_queue';
sprayDestGroup := thorlib.group();
#end

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
setupCsv := output(allPeople, , sprayPrepFileName+'_CSV', CSV, OVERWRITE, NAMED('setupCsv'));
setupXml := output(allPeople, , sprayPrepFileName+'_XML', XML('Rowtag'), OVERWRITE, NAMED('setupXml'));
setupFix := output(allPeople, , sprayPrepFileName+'_FIX', OVERWRITE, NAMED('setupFix'));


rec := RECORD
  string result;
  string msg;
end;

// Despray it to default drop zone
rec despray(rec l) := TRANSFORM
  SELF.msg := FileServices.fDespray(
                       LOGICALNAME := sprayPrepFileName+'_CSV'
                      ,DESTINATIONPATH := desprayOutFileName+'_CSV'
                      ,DESTINATIONPLANE := 'mydropzone'
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
                      ,DESTINATIONPATH := desprayOutFileName+'_XML'
                      ,DESTINATIONPLANE := 'mydropzone'
                      ,ALLOWOVERWRITE := True
                      );
  SELF.result := 'Pass';
end;

dst2 := NOFOLD(DATASET([{'', ''}], rec));
p2 := NOTHOR(PROJECT(NOFOLD(dst2), desprayXml(LEFT)));
c2 := CATCH(NOFOLD(p2), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Despray Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    desprayOutXml := output(c2,,NAMED('desprayOutXml'));
#else
    desprayOutXml := output(c2, {result},NAMED('desprayOutXml'));
#end

// Despray it to default drop zone
rec desprayFix(rec l) := TRANSFORM
  SELF.msg := FileServices.fDespray(
                       LOGICALNAME := sprayPrepFileName+'_FIX'
                      ,DESTINATIONPATH := desprayOutFileName+'_FIX'
                      ,DESTINATIONPLANE := 'mydropzone'
                      ,ALLOWOVERWRITE := True
                      );
  SELF.result := 'Pass';
end;

dst3 := NOFOLD(DATASET([{'', ''}], rec));
p3 := NOTHOR(PROJECT(NOFOLD(dst3), desprayFix(LEFT)));
c3 := CATCH(NOFOLD(p3), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Despray Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    desprayOutFix := output(c3,,NAMED('desprayOuFix'));
#else
    desprayOutFix := output(c3, {result},NAMED('desprayOuFix'));
#end



sprayRec := RECORD
  string sourceFileName;
  string destFileName;
  string dfuQueue;
  string result;
  string msg;
end;

sprayRec sprayVariable(sprayRec l) := TRANSFORM
    SELF.msg := FileServices.fSprayVariable(
                        SOURCEPLANE := 'mydropzone',
                        SOURCEPATH := l.sourceFileName,
                        DESTINATIONGROUP := sprayDestGroup,
                        DESTINATIONLOGICALNAME := l.destFileName,
                        TIMEOUT := -1,
                        ALLOWOVERWRITE := true,
                        DFUSERVERQUEUE := l.dfuQueue
                        );
    SELF.result := l.result + ' Pass';
    SELF.sourceFileName := l.sourceFileName;
    SELF.destFileName := l.destFileName;
    SELF.dfuQueue := l.dfuQueue;
end;

// Spray variable with default DFU queue
sdst1 := NOFOLD(DATASET([{desprayOutFileName + '_CSV', sprayOutFileName + '_CSV', '', 'Spray variable without queue param:', ''}], sprayRec));
sp1 := PROJECT(NOFOLD(sdst1), sprayVariable(LEFT));
sc1 := CATCH(NOFOLD(sp1), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.sourceFileName := desprayOutFileName + '_CSV',
                                 SELF.destFileName := sprayOutFileName + '_CSV',
                                 SELF.dfuQueue := '',
                                 SELF.result := 'Spray variable without queue param: Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    sprayVariableOut1 := output(sc1,,NAMED('sprayVariableOut1'));
#else
    sprayVariableOut1 := output(sc1, {result},NAMED('sprayVariableOut1'));
#end


// Spray variable with valid DFU queue
sdst2 := NOFOLD(DATASET([{desprayOutFileName + '_CSV', sprayOutFileName + '_CSV', defaultDfuQueueName, 'Spray variable with default queue:', ''}], sprayRec));
sp2 := PROJECT(NOFOLD(sdst2), sprayVariable(LEFT));
sc2 := CATCH(NOFOLD(sp2), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.sourceFileName := desprayOutFileName + '_CSV',
                                 SELF.destFileName := sprayOutFileName + '_CSV',
                                 SELF.dfuQueue := defaultDfuQueueName,
                                 SELF.result := 'Spray variable with default queue: Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    sprayVariableOut2 := output(sc2,,NAMED('sprayVariableOut2'));
#else
    sprayVariableOut2 := output(sc2, {result},NAMED('sprayVariableOut2'));
#end

// Spray variable with invalid DFU queue
sdst3 := NOFOLD(DATASET([{desprayOutFileName + '_CSV', sprayOutFileName + '_CSV', 'bela_queue', 'Spray variable with wrong queue name:', ''}], sprayRec));
sp3 := PROJECT(NOFOLD(sdst3), sprayVariable(LEFT));
sc3 := CATCH(NOFOLD(sp3), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.sourceFileName := desprayOutFileName + '_CSV',
                                 SELF.destFileName := sprayOutFileName + '_CSV',
                                 SELF.dfuQueue := 'bela_queue',
                                 SELF.result := 'Spray variable with wrong queue name: Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    sprayVariableOut3 := output(sc3,,NAMED('sprayVariableOut3'));
#else
    sprayVariableOut3 := output(sc3, {result},NAMED('sprayVariableOut3'));
#end


sprayRec sprayXml(sprayRec l) := TRANSFORM
    SELF.msg := FileServices.fSprayXml(
                            SOURCEPLANE := 'mydropzone',
                            SOURCEPATH := l.sourceFileName,
                            SOURCEROWTAG := 'Rowtag',
                            DESTINATIONGROUP := sprayDestGroup,
                            DESTINATIONLOGICALNAME := l.destFileName,
                            TIMEOUT := -1,
                            ALLOWOVERWRITE := true,
                            DFUSERVERQUEUE := l.dfuQueue
                            ); 
    SELF.result := l.result + ' Pass';
    SELF.sourceFileName := l.sourceFileName;
    SELF.destFileName := l.destFileName;
    SELF.dfuQueue := l.dfuQueue;
end;

// Spray XML with default DFU queue
sxdst1 := NOFOLD(DATASET([{desprayOutFileName + '_XML', sprayOutFileName + '_XML', '', 'Spray XML without queue param:', ''}], sprayRec));
sxp1 := PROJECT(NOFOLD(sxdst1), sprayXml(LEFT));
sxc1 := CATCH(NOFOLD(sxp1), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.sourceFileName := desprayOutFileName + '_XML',
                                 SELF.destFileName := sprayOutFileName + '_XML',
                                 SELF.dfuQueue := '',
                                 SELF.result := 'Spray XML without queue param: Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    sprayXmlOut1 := output(sxc1,,NAMED('sprayXmlOut1'));
#else
    sprayXmlOut1 := output(sxc1, {result},NAMED('sprayXmlOut1'));
#end


// Spray XML with valid DFU queue
sxdst2 := NOFOLD(DATASET([{desprayOutFileName + '_XML', sprayOutFileName + '_XML', defaultDfuQueueName, 'Spray XML with default queue:', ''}], sprayRec));
sxp2 := PROJECT(NOFOLD(sxdst2), sprayXml(LEFT));
sxc2 := CATCH(NOFOLD(sxp2), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.sourceFileName := desprayOutFileName + '_XML',
                                 SELF.destFileName := sprayOutFileName + '_XML',
                                 SELF.dfuQueue := defaultDfuQueueName,
                                 SELF.result := 'Spray XML with default queue: Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    sprayXmlOut2 := output(sxc2,,NAMED('sprayXmlOut2'));
#else
    sprayXmlOut2 := output(sxc2, {result},NAMED('sprayXmlOut2'));
#end

// Spray XML with invalid DFU queue
sxdst3 := NOFOLD(DATASET([{desprayOutFileName + '_XML', sprayOutFileName + '_XML', 'bela_queue', 'Spray XML with wrong queue name:', ''}], sprayRec));
sxp3 := PROJECT(NOFOLD(sxdst3), sprayXml(LEFT));
sxc3 := CATCH(NOFOLD(sxp3), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.sourceFileName := desprayOutFileName + '_XML',
                                 SELF.destFileName := sprayOutFileName + '_XML',
                                 SELF.dfuQueue := 'bela_queue',
                                 SELF.result := 'Spray XML with wrong queue name: Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    sprayXmlOut3 := output(sxc3,,NAMED('sprayXmlOut3'));
#else
    sprayXmlOut3 := output(sxc3, {result},NAMED('sprayXmlOut3'));
#end


sprayRec sprayFixed(sprayRec l) := TRANSFORM
    SELF.msg := FileServices.fSprayFixed(
                            SOURCEPLANE := 'mydropzone',
                            SOURCEPATH := l.sourceFileName,
                            RECORDSIZE := 9,
                            DESTINATIONGROUP := sprayDestGroup,
                            DESTINATIONLOGICALNAME := l.destFileName,
                            TIMEOUT := -1,
                            ALLOWOVERWRITE := true,
                            DFUSERVERQUEUE := l.dfuQueue
                            ); 
    SELF.result := l.result + ' Pass';
    SELF.sourceFileName := l.sourceFileName;
    SELF.destFileName := l.destFileName;
    SELF.dfuQueue := l.dfuQueue;
end;

// Spray fixed with default DFU queue
sfdst1 := NOFOLD(DATASET([{desprayOutFileName + '_FIX', sprayOutFileName + '_FIX', '', 'Spray fixed without queue param:', ''}], sprayRec));
sfp1 := PROJECT(NOFOLD(sfdst1), sprayFixed(LEFT));
sfc1 := CATCH(NOFOLD(sfp1), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.sourceFileName := desprayOutFileName + '_FIX',
                                 SELF.destFileName := sprayOutFileName + '_FIX',
                                 SELF.dfuQueue := '',
                                 SELF.result := 'Spray fixed without queue param: Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    sprayFixedOut1 := output(sfc1,,NAMED('sprayFixedOut1'));
#else
    sprayFixedOut1 := output(sfc1, {result},NAMED('sprayFixedOut1'));
#end

// Spray fixed with valid DFU queue
sfdst2 := NOFOLD(DATASET([{desprayOutFileName + '_FIX', sprayOutFileName + '_FIX', defaultDfuQueueName, 'Spray fixed with default queue:', ''}], sprayRec));
sfp2 := PROJECT(NOFOLD(sfdst2), sprayFixed(LEFT));
sfc2 := CATCH(NOFOLD(sfp2), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.sourceFileName := desprayOutFileName + '_FIX',
                                 SELF.destFileName := sprayOutFileName + '_FIX',
                                 SELF.dfuQueue := defaultDfuQueueName,
                                 SELF.result := 'Spray fixed with default queue: Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    sprayFixedOut2 := output(sfc2,,NAMED('sprayFixedOut2'));
#else
    sprayFixedOut2 := output(sfc2, {result},NAMED('sprayFixedOut2'));
#end

// Spray fixed with invalid DFU queue
sfdst3 := NOFOLD(DATASET([{desprayOutFileName + '_FIX', sprayOutFileName + '_FIX', 'bela_queue', 'Spray fixed with wrong queue name:', ''}], sprayRec));
sfp3 := PROJECT(NOFOLD(sfdst3), sprayFixed(LEFT));
sfc3 := CATCH(NOFOLD(sfp3), ONFAIL(TRANSFORM(sprayRec,
                                 SELF.sourceFileName := desprayOutFileName + '_FIX',
                                 SELF.destFileName := sprayOutFileName + '_FIX',
                                 SELF.dfuQueue := 'bela_queue',
                                 SELF.result := 'Spray fixed with wrong queue name: Fail',
                                 SELF.msg := FAILMESSAGE
                                )));

#if (VERBOSE = 1)
    sprayFixedOut3 := output(sfc3,,NAMED('sprayFixedOut3'));
#else
    sprayFixedOut3 := output(sfc3, {result},NAMED('sprayFixedOut3'));
#end



sequential (
    //output(dropzonePath, NAMED('dropzonePath')),
    // Preparation
    setupCsv,
    setupXml,
    setupFix,
    
    desprayOutCsv,
    desprayOutXml,
    desprayOutFix,

    // Spray tests
    sprayVariableOut1,
    sprayVariableOut2,
    sprayVariableOut3,
    
    sprayXmlOut1,
    sprayXmlOut2,
    sprayXmlOut3,
    
    sprayFixedOut1,
    sprayFixedOut2,
    sprayFixedOut3,

    // Clean-up
    FileServices.DeleteExternalFile('.', FileServices.GetDefaultDropZone() + '/' + desprayOutFileName+'_CSV'),
    FileServices.DeleteExternalFile('.', FileServices.GetDefaultDropZone() + '/' + desprayOutFileName+'_XML'),
    FileServices.DeleteExternalFile('.', FileServices.GetDefaultDropZone() + '/' + desprayOutFileName+'_FIX'),
    
    FileServices.DeleteLogicalFile(sprayPrepFileName+'_CSV'),
    FileServices.DeleteLogicalFile(sprayPrepFileName+'_XML'),
    FileServices.DeleteLogicalFile(sprayPrepFileName+'_FIX'),
    
    FileServices.DeleteLogicalFile(sprayOutFileName+'_CSV'),
    FileServices.DeleteLogicalFile(sprayOutFileName+'_XML'),
    FileServices.DeleteLogicalFile(sprayOutFileName + '_FIX'),
    
);

