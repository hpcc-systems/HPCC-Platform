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
import $.setup;
prefix := setup.Files(false, false).FilePrefix;

// This is not an engine test, but a DFU.
// Doesn't matter much which engine does it, so we restrict to only one

//noRoxie
//noThorLCR

//class=spray

//version isTerminated=false
//version isTerminated=true

import ^ as root;

isTerminated := #IFDEFINED(root.isTerminated, false);

unsigned VERBOSE := 0;

Layout := RECORD
    STRING field1;
    STRING field2;
    STRING field3;
    STRING field4;
    STRING field5;
END;

header := DATASET([{'Id', 'Field1', 'Field2', 'Field3', 'Field4'}], Layout);

#if (isTerminated)
    sprayPrepFileName := prefix + 'spray_prep_terminated';
    // Create a one record CSV logical file with terminator a the end
    setupFile := output(header, , sprayPrepFileName, CSV, OVERWRITE);

    desprayOutFileName := '/var/lib/HPCCSystems/mydropzone/spray_input_terminated';
    sprayOutFileName := prefix + 'spray_test_terminated';
#else
    sprayPrepFileName := prefix + 'spray_prep_not_terminated';
    // Create a one record CSV logical file without terminator a the end
    setupFile := output(header, , sprayPrepFileName, CSV(TERMINATOR('')), OVERWRITE);

    desprayOutFileName := '/var/lib/HPCCSystems/mydropzone/spray_input_not_terminated';
    sprayOutFileName := prefix + 'spray_test_not_terminated';
#end

rec := RECORD
  string result;
  string msg;
end;


// Despray it to default drop zone to create one record/row CVS file w/wo terminator
rec despray(rec l) := TRANSFORM
  SELF.msg := FileServices.fDespray(
                       LOGICALNAME := sprayPrepFileName
                      ,DESTINATIONIP := '.'
                      ,DESTINATIONPATH := desprayOutFileName
                      ,ALLOWOVERWRITE := True
                      );
  SELF.result := 'Despray Pass';
end;

dst1 := NOFOLD(DATASET([{'', ''}], rec));
p1 := PROJECT(NOFOLD(dst1), despray(LEFT));
c1 := CATCH(NOFOLD(p1), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Despray Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    desprayOut := output(c1);
#else
    desprayOut := output(c1, {result});
#end


// Spray the one record/row CVS file w/wo terminator file to check
// DFU spray handles it well.
rec spray(rec l) := TRANSFORM
    SELF.msg := FileServices.fSprayVariable(
                        SOURCEIP := '.',
                        SOURCEPATH := desprayOutFileName,
                        //RECORDSIZE := RecordSize,
                        DESTINATIONGROUP := 'mythor',
                        DESTINATIONLOGICALNAME := sprayOutFileName,
                        TIMEOUT := -1,
                        ESPSERVERIPPORT := 'http://127.0.0.1:8010/FileSpray',
                        ALLOWOVERWRITE := true
                        );
    self.result := 'Spray Pass';
end;

dst2 := NOFOLD(DATASET([{'', ''}], rec));
p2 := PROJECT(NOFOLD(dst2), spray(LEFT));
c2 := CATCH(NOFOLD(p2), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Spray Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    sprayOut := output(c2);
#else
    sprayOut := output(c2, {result});
#end

ds := DATASET(sprayOutFileName, Layout, csv);

string compareDatasets(dataset(Layout) ds1, dataset(Layout) ds2) := FUNCTION
   boolean result := (0 = COUNT(JOIN(ds1, ds2, left.field1=right.field1, FULL ONLY)));
   RETURN if(result, 'Pass', 'Fail');
END;


SEQUENTIAL(
    setupFile,
    desprayOut,
    sprayOut,
    output(compareDatasets(header,ds)),

    // Clean-up
    FileServices.DeleteExternalFile('.', desprayOutFileName),
    FileServices.DeleteLogicalFile(sprayOutFileName),
    FileServices.DeleteLogicalFile(sprayPrepFileName),
);
