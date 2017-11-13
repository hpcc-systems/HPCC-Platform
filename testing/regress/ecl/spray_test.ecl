/*##############################################################################

    Copyright (C) 2012 HPCC Systems®.

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
//noThor

//class=spray

//version sprayFixed=true
//version sprayFixed=false,sprayEmpty=false
//version sprayFixed=false,sprayEmpty=true

import ^ as root;

boolean sprayFixed := #IFDEFINED(root.sprayFixed, true);
boolean sprayEmpty := #IFDEFINED(root.sprayEmpty, false);

unsigned VERBOSE := 0;

Layout_Person := RECORD
  STRING3  name;
  UNSIGNED2 age;
  BOOLEAN good;
END;

empty := DATASET([], Layout_Person);

allPeople := DATASET([ {'foo', 10, 1},
                       {'bar', 12, 0},
                       {'baz', 32, 1}]
            ,Layout_Person);

#if (sprayFixed)
    sprayPrepFileName := prefix + 'spray_prep_fixed';
    desprayOutFileName := '/var/lib/HPCCSystems/mydropzone/spray_input_fixed';
    sprayOutFileName := prefix + 'spray_test_fixed';
    dsSetup := allPeople;
#else
    #if (sprayEmpty)
        sprayPrepFileName := prefix + 'spray_prep_empty';
        desprayOutFileName := '/var/lib/HPCCSystems/mydropzone/spray_input_empty';
        sprayOutFileName := prefix + 'spray_test_empty';
        dsSetup := empty;
    #else
        sprayPrepFileName := prefix + 'spray_prep';
        desprayOutFileName := '/var/lib/HPCCSystems/mydropzone/spray_input';
        sprayOutFileName := prefix + 'spray_test';
        dsSetup := allPeople;
    #end
#end

//  Create a small logical file
setupFile := output(dsSetup, , sprayPrepFileName, CSV, OVERWRITE);

rec := RECORD
  string result;
  string msg;
end;


// Despray it to default drop zone
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


#if (sprayFixed = true)
    rec spray(rec l) := TRANSFORM
        SELF.msg := FileServices.fSprayFixed(
                            SOURCEIP := '.',
                            SOURCEPATH := desprayOutFileName,
                            RECORDSIZE := 9,
                            DESTINATIONGROUP := 'mythor',
                            DESTINATIONLOGICALNAME := sprayOutFileName,
                            TIMEOUT := -1,
                            ESPSERVERIPPORT := 'http://127.0.0.1:8010/FileSpray',
                            ALLOWOVERWRITE := true
                            );
        self.result := 'Spray Pass';
    end;
#else
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
#end


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

ds := DATASET(sprayOutFileName, Layout_Person, csv);

string compareDatasets(dataset(Layout_Person) ds1, dataset(Layout_Person) ds2) := FUNCTION
   boolean result := (0 = COUNT(JOIN(ds1, ds2, left.name=right.name, FULL ONLY)));
   RETURN if(result, 'Pass', 'Fail');
END;




SEQUENTIAL(
  setupFile,
  desprayOut,
  sprayOut,
  output(compareDatasets(dsSetup,ds))
);
