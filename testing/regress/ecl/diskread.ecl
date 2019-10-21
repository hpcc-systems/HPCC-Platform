/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
//version multiPart=false
//version multiPart=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

#option ('layoutTranslation', useTranslation);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

// straight disk count
#option('workflow', 0);
/*
count(Files.DG_FlatFile);
count(Files.DG_FlatFileEvens);
//count(Files.DG_CSVFile);
//count(Files.DG_XMLFile);
count(Files.DG_VarFile);

// straight disk read

COUNT(DEDUP(Files.DG_FlatFile, RECORD));
COUNT(DEDUP(Files.DG_FlatFileEvens, RECORD));
//COUNT(DEDUP(Files.DG_CSVFile, RECORD));
//COUNT(DEDUP(Files.DG_XMLFile, RECORD));
COUNT(DEDUP(Files.DG_VarFile, RECORD));

// filtered disk read

output(Files.DG_FlatFile(DG_firstname='CLAIRE'));
output(Files.DG_FlatFileEvens(DG_firstname='CLAIRE'));
//output(Files.DG_CSVFile(DG_firstname='CLAIRE'));
//output(Files.DG_XMLFile(DG_firstname='CLAIRE'));
output(Files.DG_VarFile(DG_firstname='CLAIRE'));

// keyed disk read

//output(Files.DG_FlatFile(KEYED(DG_firstname='CLAIRE')));
//output(Files.DG_FlatFileEvens(KEYED(DG_firstname='CLAIRE')));
//output(Files.DG_CSVFile(KEYED(DG_firstname='CLAIRE')));
//output(Files.DG_XMLFile(KEYED(DG_firstname='CLAIRE')));
//output(Files.DG_VarFile(KEYED(DG_firstname='CLAIRE')));
*/

// now repeat with preload....


DG_FlatFile_pl0 := PRELOAD(Files.DG_FlatFile);
DG_FlatFileEvens_pl0 := PRELOAD(Files.DG_FlatFileEvens);
//DG_CSVFile_pl0 := PRELOAD(Files.DG_CSVFile);
//DG_XMLFile_pl0 := PRELOAD(Files.DG_XMLFile);
//DG_VarFile_pl0 := PRELOAD(Files.DG_VarFile);

// straight disk count

count(DG_FlatFile_pl0);
count(DG_FlatFileEvens_pl0);
//count(DG_CSVFile_pl0);
//count(DG_XMLFile_pl0);
//count(DG_VarFile_pl0);

// straight disk read

COUNT(DEDUP(DG_FlatFile_pl0, RECORD));
COUNT(DEDUP(DG_FlatFileEvens_pl0, RECORD));
//COUNT(DEDUP(DG_CSVFile_pl0, RECORD));
//COUNT(DEDUP(DG_XMLFile_pl0, RECORD));
//COUNT(DEDUP(DG_VarFile_pl0, RECORD));

// filtered disk read

output(DG_FlatFile_pl0(DG_firstname='CLAIRE'));
output(DG_FlatFileEvens_pl0(DG_firstname='CLAIRE'));
//output(DG_CSVFile_pl0(DG_firstname='CLAIRE'));
//output(DG_XMLFile_pl0(DG_firstname='CLAIRE'));
//output(DG_VarFile_pl0(DG_firstname='CLAIRE'));

// keyed disk read

output(DG_FlatFile_pl0(KEYED(DG_firstname='CLAIRE')));
output(DG_FlatFileEvens_pl0(KEYED(DG_firstname='CLAIRE')));
//output(DG_CSVFile_pl0(KEYED(DG_firstname='CLAIRE')));
//output(DG_XMLFile_pl0(KEYED(DG_firstname='CLAIRE')));
//output(DG_VarFile_pl0(KEYED(DG_firstname='CLAIRE')));

/*
// now repeat with preload(n)....

DG_FlatFile_pl2 := PRELOAD(Files.DG_FlatFile, 2);
DG_FlatFileEvens_pl2 := PRELOAD(Files.DG_FlatFileEvens, 2);
//DG_CSVFile_pl2 := PRELOAD(Files.DG_CSVFile, 2);
//DG_XMLFile_pl2 := PRELOAD(Files.DG_XMLFile, 2);
DG_VarFile_pl2 := PRELOAD(Files.DG_VarFile, 2);

// straight disk count

count(DG_FlatFile_pl2);
count(DG_FlatFileEvens_pl2);
//count(DG_CSVFile_pl2);
//count(DG_XMLFile_pl2);
count(DG_VarFile_pl2);

// straight disk read

COUNT(DEDUP(DG_FlatFile_pl2, RECORD));
COUNT(DEDUP(DG_FlatFileEvens_pl2, RECORD));
//COUNT(DEDUP(DG_CSVFile_pl2, RECORD));
//COUNT(DEDUP(DG_XMLFile_pl2, RECORD));
COUNT(DEDUP(DG_VarFile_pl2, RECORD));

// filtered disk read

output(DG_FlatFile_pl2(DG_firstname='CLAIRE'));
output(DG_FlatFileEvens_pl2(DG_firstname='CLAIRE'));
//output(DG_CSVFile_pl2(DG_firstname='CLAIRE'));
//output(DG_XMLFile_pl2(DG_firstname='CLAIRE'));
output(DG_VarFile_pl2(DG_firstname='CLAIRE'));

// keyed disk read

output(DG_FlatFile_pl2(KEYED(DG_firstname='CLAIRE')));
output(DG_FlatFileEvens_pl2(KEYED(DG_firstname='CLAIRE')));
//output(DG_CSVFile_pl2(KEYED(DG_firstname='CLAIRE')));
//output(DG_XMLFile_pl2(KEYED(DG_firstname='CLAIRE')));
//output(DG_VarFile_pl2(KEYED(DG_firstname='CLAIRE')));

*/

// MORE - tests: make sure that we can handle a combination of keyed/unkeyed filtering...
// MORE - preload and non-preload for the same dataset is not really testing what you think, in roxie
// MORE - in fact, unless queries get deleted, roxie will use first sight of the file even if we separate into multiple .ecl files.
