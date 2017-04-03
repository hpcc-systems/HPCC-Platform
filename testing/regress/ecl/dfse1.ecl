/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

//class=file
//class=error
//fail

import $.setup;
Files := setup.Files(false, false, false);

// Test compile-time field translation - error cases

Slim_OutRec := RECORD
    string10  DG_firstname;
    string10  DG_lastname;
END;

Extra_OutRec := RECORD
    string10  DG_firstname;
    string3   DG_lastname;
    string1   newfield { DEFAULT('Y')};
END;

#option ('reportDFSinfo',2);

// Test compile-time field translation - error cases for RECORDOF variant

// File not constant
notconstF := nofold(Files.DG_ParentFileOut+'missing');
notconst := DATASET(notconstF,Extra_OutRec,FLAT,LOOKUP);
output(notconst[1]);
notconstR := RECORDOF(notconstF,Extra_OutRec,LOOKUP);
notconstRD := DATASET(notconstF,notconstR,FLAT);
output(notconstRD[1]);

// File constant but not foldable
set of string10 xAll := ALL + ['x'];
notfoldableF := xall[1];
notfoldable := DATASET(notfoldableF,Extra_OutRec,FLAT, LOOKUP);
output(notfoldable[1]);
notfoldableR := RECORDOF(notfoldableF,Extra_OutRec,LOOKUP);
notfoldableRD := DATASET(notfoldableF,notfoldableR,FLAT);
output(notfoldableRD[1]);

// File not present
notthereF := Files.DG_ParentFileOut+'missing';
notthereR := RECORDOF(notthereF,Extra_OutRec,LOOKUP);
notthereRD := DATASET(notthereF,notfoldableR,FLAT);
output(notthereRD[1]);

// File has incompatible matching fields
badmatchingfields1R := DATASET(Files.DG_ParentFileOut, RECORDOF(Files.DG_ParentFileOut, { unsigned1 DG_firstname { DEFAULT(1) } }, LOOKUP),FLAT);
output(badmatchingfields1R[1]);

// Inside a nested record versus outside - this is actually allowed
badmatchingfields2R := DATASET(Files.DG_ParentFileOut, RECORDOF(Files.DG_ParentFileOut, { { STRING10 DG_firstname } }, LOOKUP),FLAT);
output(badmatchingfields2R[1]);

// Record/dataset becomes field or vice versa - not allowed

childrec := RECORD
  string10 child;
END;
badmatchingfields3R := DATASET(Files.DG_ParentFileOut, RECORDOF(Files.DG_ParentFileOut, { childrec DG_firstname }, LOOKUP),FLAT);
output(badmatchingfields3R[1]);
