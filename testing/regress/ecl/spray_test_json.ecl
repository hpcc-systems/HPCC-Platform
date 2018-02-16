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

//version isSmallFile=true,isUnBallanced=false
//version isSmallFile=true,isUnBallanced=true
//version isSmallFile=false

import std.system.thorlib;
import Std.File AS FileServices;
import ^ as root;

jlib:= SERVICE
    unsigned8 rtlTick() : library='jlib',eclrtl,entrypoint='rtlNano';
END;

isSmallFile := #IFDEFINED(root.isSmallFile, true);

isUnBallanced := #IFDEFINED(root.isUnBallanced, false);

dropzonePath := '/var/lib/HPCCSystems/mydropzone/' : STORED('dropzonePath');
engine := thorlib.platform() : stored('thor');
prefix := engine + '-';
suffix := '-' + jlib.rtlTick() : stored('startTime');
nodes := thorlib.nodes();

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
OriginalDataFile := prefix + File + suffix;

//  Outputs  ---
setupPeople := OUTPUT(somePeople,,OriginalDataFile, JSON, OVERWRITE);


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
DesprayTargetFile1 := dropzonePath + File + suffix;
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


sprayRec := RECORD
  string result;
  string msg;
end;

SprayTargetFileName := prefix + 'spray_test-' + suffix;

//To spray a JSON file we use XML Spray
sprayRec doSpray(sprayRec l) := TRANSFORM
    SELF.msg := FileServices.fSprayXml(
                                SOURCEIP := '.',
                                SOURCEPATH := DesprayTargetFile1,
                                SOURCEROWTAG := 'Row',
                                DESTINATIONGROUP := 'my'+engine,
                                DESTINATIONLOGICALNAME := SprayTargetFileName,
                                TIMEOUT := -1,
                                ESPSERVERIPPORT := 'http://127.0.0.1:8010/FileSpray',
                                ALLOWOVERWRITE := true
                                );
    self.result := 'Spray Pass';
end;


dst3 := NOFOLD(DATASET([{'', ''}], sprayRec));

p3 := NOTHOR(PROJECT(NOFOLD(dst3), doSpray(LEFT)));
c3 := CATCH(NOFOLD(p3), ONFAIL(TRANSFORM(sprayRec,
                                  SELF.result := 'Spray Fail',
                                  SELF.msg := FAILMESSAGE
                                 )));
#if (VERBOSE = 1)
    spray := output(c3);
#else
    spray := output(c3, {result});
#end


ds := DATASET(SprayTargetFileName, Layout_Person, JSON('Row'));

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

    // Clean-up
    FileServices.DeleteLogicalFile(OriginalDataFile),
    FileServices.DeleteLogicalFile(SprayTargetFileName),
    FileServices.DeleteExternalFile('.', DesprayTargetFile1),

);
