/*##############################################################################

    Copyright (C) 2018 HPCC Systems®.

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

#onwarning(10138, ignore);

//version isSmallFile=true,isUnBallanced=false
//version isSmallFile=true,isUnBallanced=true
//version isSmallFile=false

import std.system.thorlib;
import Std.File AS FileServices;
import $.setup;
import ^ as root;


isSmallFile := #IFDEFINED(root.isSmallFile, true);

isUnBallanced := #IFDEFINED(root.isUnBallanced, false);

dropzonePath := FileServices.GetDefaultDropZone() +'/' : STORED('dropzonePath');
engine := thorlib.platform() : stored('thor');
prefix := setup.Files(false, false).FilePrefix + '-' + WORKUNIT;
nodes := thorlib.nodes();
espUrl := FileServices.GetEspURL() + '/FileSpray';

unsigned VERBOSE := 0;

Layout_Person := RECORD
  UNSIGNED4 PersonID;
  STRING    FirstName;
  STRING25  LastName;
END;

allPeople := DATASET([ {1,'Fred','Smith'},
                       {2,'Joe','Blow'},
                       {3,'Jane','Smith'}],Layout_Person);

// One record, object is much larger tha the others. It is testing the
// split point calculation on multi target environment.
unBallanced := DATASET([ {1,'Fred','Smith'},
                       {2,'Joe_012345678901234567890912345678901234567890123456789001234567890123456789','Blow'},
                       {3,'Jane','Smith'}],Layout_Person);

manyPeople := DATASET(nodes * 100,
              TRANSFORM({Layout_Person},
                         SELF.PersonID := COUNTER;
                         SELF.FirstName := allPeople[(COUNTER-1) % 3 + 1].FirstName;
                         SELF.LastName := allPeople[(COUNTER-1) % 3 + 1].LastName;
                        )
              ,DISTRIBUTED
              );

#if (isSmallFile)
    somePeople := if (nodes = 1,
                        allPeople(LastName = 'Smith'),
                    #if (isUnBallanced)
                         unBallanced
                    #else
                        allPeople(LastName = 'Blow')
                    #end
                    );
#else
    somePeople := manyPeople;
#end

SrcAddrIp := '.';
File := 'persons';
OriginalDataFile := prefix + File;
OriginalDataFile2 := prefix + File + '2';

//  Outputs  ---
setupPeople := OUTPUT(somePeople,,DYNAMIC(OriginalDataFile), JSON, OVERWRITE);
setupPeople2 := OUTPUT(somePeople,,DYNAMIC(OriginalDataFile2),  JSON('', HEADING('', ''), OPT, TRIM), OVERWRITE);

ClusterName := 'mythor';

desprayRec := RECORD
   string sourceFile;
   string destFile;
   string ip;
   boolean allowOverwrite;
   string result;
   string msg;
 end;

desprayRec doDespray(desprayRec l) := TRANSFORM
   SELF.sourceFile := l.sourceFile;
   SELF.msg := FileServices.fDespray(l.sourceFile
                                          ,l.ip
                                          ,destinationPath := l.destFile
                                          ,ALLOWOVERWRITE := l.allowOverwrite
                                          );
   SELF.result := 'Despray Pass';
   SELF.ip := l.ip;
   SELF.allowOverwrite := l.allowOverwrite;
   SELF.destFile := l.destFile;
 end;

// This should be fine based on valid target file path and SrcAddIp
DesprayTargetFile1 := dropzonePath + WORKUNIT + '-' + File;
dst2 := NOFOLD(DATASET([{OriginalDataFile, DesprayTargetFile1, SrcAddrIp, True, '', ''}], desprayRec));

p2 := NOTHOR(PROJECT(NOFOLD(dst2), doDespray(LEFT)));

c2 := CATCH(NOFOLD(p2), ONFAIL(TRANSFORM(desprayRec,
                                  SELF.sourceFile := OriginalDataFile,
                                  SELF.destFile := DesprayTargetFile1,
                                  SELF.ip := SrcAddrIp,
                                  SELF.allowOverwrite := True,
                                  SELF.result := 'Fail',
                                  SELF.msg := FAILMESSAGE
                                 )));
#if (VERBOSE = 1)
     despray := output(c2);
#else
     despray := output(c2, {result});
#end


// This should be fine based on valid target file path and SrcAddIp
DesprayTargetFile2 := dropzonePath + WORKUNIT + '-' + File + '2';
dst2b := NOFOLD(DATASET([{OriginalDataFile, DesprayTargetFile2, SrcAddrIp, True, '', ''}], desprayRec));

p2b := NOTHOR(PROJECT(NOFOLD(dst2b), doDespray(LEFT)));

c2b := CATCH(NOFOLD(p2b), ONFAIL(TRANSFORM(desprayRec,
                                  SELF.sourceFile := OriginalDataFile,
                                  SELF.destFile := DesprayTargetFile2,
                                  SELF.ip := SrcAddrIp,
                                  SELF.allowOverwrite := True,
                                  SELF.result := 'Fail',
                                  SELF.msg := FAILMESSAGE
                                 )));
#if (VERBOSE = 1)
     despray2 := output(c2b);
#else
     despray2 := output(c2b, {result});
#end



sprayRec := RECORD
  string sourceFileName;
  string targetFileName;
  string result;
  string msg;
end;

//To spray a JSON file we use JSON Spray
sprayRec doSpray(sprayRec l) := TRANSFORM
    SELF.sourceFileName := l.sourceFileName;
    SELF.targetFileName := l.targetFileName;
    SELF.msg := FileServices.fSprayJson(
                                SOURCEIP := '.',
                                SOURCEPATH := l.sourceFileName,
                                DESTINATIONGROUP := 'my'+engine,
                                DESTINATIONLOGICALNAME := l.targetFileName,
                                TIMEOUT := -1,
                                ESPSERVERIPPORT := espUrl,
                                ALLOWOVERWRITE := true
                                );
    self.result := 'Spray Pass';
end;


SprayTargetFileName1 := prefix + 'spray_test';
dst3 := NOFOLD(DATASET([{DesprayTargetFile1, SprayTargetFileName1, '', ''}], sprayRec));

p3 := NOTHOR(PROJECT(NOFOLD(dst3), doSpray(LEFT)));
c3 := CATCH(NOFOLD(p3), ONFAIL(TRANSFORM(sprayRec,
                                  SELF.sourceFileName := DesprayTargetFile1,
                                  SELF.targetFileName := SprayTargetFileName1,
                                  SELF.result := 'Spray Fail',
                                  SELF.msg := FAILMESSAGE
                                 )));
#if (VERBOSE = 1)
    spray := output(c3);
#else
    spray := output(c3, {result});
#end



SprayTargetFileName2 := prefix + 'spray_test2';
dst3b := NOFOLD(DATASET([{DesprayTargetFile2, SprayTargetFileName2, '', ''}], sprayRec));

p3b := NOTHOR(PROJECT(NOFOLD(dst3b), doSpray(LEFT)));
c3b := CATCH(NOFOLD(p3b), ONFAIL(TRANSFORM(sprayRec,
                                  SELF.sourceFileName := DesprayTargetFile2,
                                  SELF.targetFileName := SprayTargetFileName2,
                                  SELF.result := 'Spray Fail',
                                  SELF.msg := FAILMESSAGE
                                 )));
#if (VERBOSE = 1)
    spray2 := output(c3b);
#else
    spray2 := output(c3b, {result});
#end



ds := DATASET(DYNAMIC(SprayTargetFileName1), Layout_Person, JSON('Row'));
ds2 := DATASET(DYNAMIC(SprayTargetFileName2), Layout_Person, JSON('Row'));

string compareDatasets(dataset(Layout_Person) ds1, dataset(Layout_Person) ds2) := FUNCTION
   boolean result := (0 = COUNT(JOIN(ds1, ds2, left.PersonID=right.PersonID, FULL ONLY)));
   RETURN if(result, 'Compare Pass', 'Fail');
END;

SEQUENTIAL(

#if (VERBOSE = 1)
    output(isSmallFile, NAMED('isSmallFile'));
    output(isUnBallanced, NAMED('isUnBallanced'));
    output(somePeople, NAMED('somePeople')),
#end

    setupPeople,
    despray,
    spray,

#if (VERBOSE = 1)
    output(ds, NAMED('ds')),
#end

    output(compareDatasets(somePeople,ds)),


    setupPeople2,
    despray2,
    spray2,

#if (VERBOSE = 1)
    output(ds2, NAMED('ds2')),
#end

    output(compareDatasets(somePeople,ds2)),

    // Clean-up
    FileServices.DeleteLogicalFile(OriginalDataFile),
    FileServices.DeleteExternalFile('.', DesprayTargetFile1),
    FileServices.DeleteLogicalFile(SprayTargetFileName1),

    FileServices.DeleteLogicalFile(OriginalDataFile2),
    FileServices.DeleteExternalFile('.', DesprayTargetFile2),
    FileServices.DeleteLogicalFile(SprayTargetFileName2),

);
