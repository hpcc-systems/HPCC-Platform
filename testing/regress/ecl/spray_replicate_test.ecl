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

//noHThor

//class=spray

import std.system.thorlib;
import Std.File AS FileServices;

dropzonePath := '/var/lib/HPCCSystems/mydropzone/' : STORED('dropzonePath');
espIpPort := 'http://127.0.0.1:8010/FileSpray' : STORED('espIpPort');
engine := thorlib.platform();
prefix := '~regress::' + engine + '::' + WORKUNIT + '::';

sprayPrepFileName := prefix + 'spray_prep';
desprayOutFileName := dropzonePath + WORKUNIT + '-file_for_spray';
sprayOutFileName := prefix + 'spray_wo_replication';
sprayDestGroup := thorlib.group();

unsigned VERBOSE := 0;

Layout_Person := RECORD
  STRING3  name;
  UNSIGNED2 age;
  BOOLEAN good;
END;

empty := DATASET([], Layout_Person);

dsSetup := DATASET([ {'foo', 10, 1},
                     {'bar', 12, 0},
                     {'baz', 32, 1} ]
            ,Layout_Person);

//  Create a small logical file
setup := output(dsSetup, , DYNAMIC(sprayPrepFileName), CSV, OVERWRITE);

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
p1 := NOTHOR(PROJECT(NOFOLD(dst1), despray(LEFT)));
c1 := CATCH(NOFOLD(p1), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Despray Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    desprayOut := output(c1);
#else
    desprayOut := output(c1, {result});
#end

rec spray(rec l) := TRANSFORM
    SELF.msg := FileServices.fSprayDelimited(
                        SOURCEIP := '.',
                        SOURCEPATH := desprayOutFileName,
                        DESTINATIONGROUP := sprayDestGroup,
                        DESTINATIONLOGICALNAME := sprayOutFileName,
                        TIMEOUT := -1,
                        ESPSERVERIPPORT := espIpPort,
                        ALLOWOVERWRITE := true,
                        REPLICATE := false
                        );
    self.result := 'Spray Pass';
end;


dst2 := NOFOLD(DATASET([{'', ''}], rec));
p2 := NOTHOR(PROJECT(NOFOLD(dst2), spray(LEFT)));
c2 := CATCH(NOFOLD(p2), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Spray Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    sprayOut := output(c2);
#else
    sprayOut := output(c2, {result});
#end


// Replicate
rec replicate(rec l) := TRANSFORM
    SELF.msg := FileServices.fReplicate(
                        LOGICALNAME := sprayOutFileName,
                        TIMEOUT := -1,
                        ESPSERVERIPPORT := espIpPort
                        );
    self.result := 'Replicate Pass';
end;


dst3 := NOFOLD(DATASET([{'', ''}], rec));
p3 := NOTHOR(PROJECT(NOFOLD(dst3), replicate(LEFT)));
c3 := CATCH(NOFOLD(p3), ONFAIL(TRANSFORM(rec,
                                 SELF.result := 'Replicate Fail',
                                 SELF.msg := FAILMESSAGE
                                )));
#if (VERBOSE = 1)
    replicateOut := output(c3);
#else
    replicateOut := output(c3, {result});
#end

SEQUENTIAL(

  setup,
  desprayOut,
  sprayOut,
  replicateOut,

  // Clean-up
  FileServices.DeleteExternalFile('.', desprayOutFileName),
  FileServices.DeleteLogicalFile(sprayPrepFileName),
  FileServices.DeleteLogicalFile(sprayOutFileName),

);
