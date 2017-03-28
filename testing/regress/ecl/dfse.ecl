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

// Test compile-time field translation - warning cases (for ones that would be picked up after initial parse)

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

// File not constant, File constant but not foldable tested in dfse1.ecl as otherwise they end up suppressing the errors below

// File not present
notthereF := Files.DG_ParentFileOut+'missing';
notthere := DATASET(notthereF,Extra_OutRec,FLAT,LOOKUP);
output(notthere[1]);

// File has no record info - tested manually
// File has partial/invalid record info - tested manually

// File has no matching fields - not technically an error but merits a warning at least!
nomatchingfields := DATASET(Files.DG_ParentFileOut, { unsigned1 constfield { DEFAULT(1) } },FLAT, LOOKUP);
output(nomatchingfields[1]);

// File has incompatible matching fields
badmatchingfields1 := DATASET(Files.DG_ParentFileOut, { unsigned1 DG_firstname { DEFAULT(1) } },FLAT, LOOKUP);
output(badmatchingfields1[1]);

// Inside a nested record versus outside - this is actually allowed

badmatchingfields2 := DATASET(Files.DG_ParentFileOut, { { STRING10 DG_firstname  } },FLAT, LOOKUP);
output(badmatchingfields2[1]);

// Record/dataset becomes field or vice versa - not allowed

childrec := RECORD
  string10 child;
END;
badmatchingfields3 := DATASET(Files.DG_ParentFileOut, { childrec DG_firstname },FLAT, LOOKUP);
output(badmatchingfields3[1]);