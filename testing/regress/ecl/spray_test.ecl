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

// This is not an engine test, but a DFU.
// Doesn't matter much which engine does it, so we restrict to only one

//noRoxie
//noThorLCR
//noThor

//version sprayFixed=true
//version sprayFixed=false

import ^ as root;

boolean sprayFixed := #IFDEFINED(root.sprayFixed, true);

unsigned VERBOSE := 0;

Layout_Person := RECORD
  STRING3  name;
  UNSIGNED2 age;
  BOOLEAN good;
END;

sprayPrepFileName := '~::spray_prep';
desprayOutFileName := '/var/lib/HPCCSystems/mydropzone/spray_input';
sprayOutFileName := '~::spray_test';

allPeople := DATASET([ {'foo', 10, 1},
                       {'bar', 12, 0},
                       {'baz', 32, 1}]
            ,Layout_Person);

//  Create a small logical file
setup := output(allPeople, , sprayPrepFileName, CSV, OVERWRITE);

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
   boolean result := (0 = COUNT(JOIN(ds1, ds2, left.name=right.name, FULL ONLY, LOCAL)));
   RETURN if(result, 'Pass', 'Fail');
END;




SEQUENTIAL(
  setup,
  desprayOut,
  sprayOut,
  output(compareDatasets(allPeople,ds))
);
