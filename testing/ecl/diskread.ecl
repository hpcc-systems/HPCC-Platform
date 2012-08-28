/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//UseStandardFiles
// straight disk count
#option('workflow', 0);
/*
count(DG_FlatFile);
count(DG_FlatFileEvens);
//count(DG_CSVFile);
//count(DG_XMLFile);
count(DG_VarFile);

// straight disk read

COUNT(DEDUP(DG_FlatFile, RECORD));
COUNT(DEDUP(DG_FlatFileEvens, RECORD));
//COUNT(DEDUP(DG_CSVFile, RECORD));
//COUNT(DEDUP(DG_XMLFile, RECORD));
COUNT(DEDUP(DG_VarFile, RECORD));

// filtered disk read

output(DG_FlatFile(DG_firstname='CLAIRE'));
output(DG_FlatFileEvens(DG_firstname='CLAIRE'));
//output(DG_CSVFile(DG_firstname='CLAIRE'));
//output(DG_XMLFile(DG_firstname='CLAIRE'));
output(DG_VarFile(DG_firstname='CLAIRE'));

// keyed disk read

//output(DG_FlatFile(KEYED(DG_firstname='CLAIRE')));
//output(DG_FlatFileEvens(KEYED(DG_firstname='CLAIRE')));
//output(DG_CSVFile(KEYED(DG_firstname='CLAIRE')));
//output(DG_XMLFile(KEYED(DG_firstname='CLAIRE')));
//output(DG_VarFile(KEYED(DG_firstname='CLAIRE')));
*/

// now repeat with preload....


DG_FlatFile_pl0 := PRELOAD(DG_FlatFile);
DG_FlatFileEvens_pl0 := PRELOAD(DG_FlatFileEvens);
//DG_CSVFile_pl0 := PRELOAD(DG_CSVFile);
//DG_XMLFile_pl0 := PRELOAD(DG_XMLFile);
DG_VarFile_pl0 := PRELOAD(DG_VarFile);

// straight disk count

count(DG_FlatFile_pl0);
count(DG_FlatFileEvens_pl0);
//count(DG_CSVFile_pl0);
//count(DG_XMLFile_pl0);
count(DG_VarFile_pl0);

// straight disk read

COUNT(DEDUP(DG_FlatFile_pl0, RECORD));
COUNT(DEDUP(DG_FlatFileEvens_pl0, RECORD));
//COUNT(DEDUP(DG_CSVFile_pl0, RECORD));
//COUNT(DEDUP(DG_XMLFile_pl0, RECORD));
COUNT(DEDUP(DG_VarFile_pl0, RECORD));

// filtered disk read

output(DG_FlatFile_pl0(DG_firstname='CLAIRE'));
output(DG_FlatFileEvens_pl0(DG_firstname='CLAIRE'));
//output(DG_CSVFile_pl0(DG_firstname='CLAIRE'));
//output(DG_XMLFile_pl0(DG_firstname='CLAIRE'));
output(DG_VarFile_pl0(DG_firstname='CLAIRE'));

// keyed disk read

output(DG_FlatFile_pl0(KEYED(DG_firstname='CLAIRE')));
output(DG_FlatFileEvens_pl0(KEYED(DG_firstname='CLAIRE')));
//output(DG_CSVFile_pl0(KEYED(DG_firstname='CLAIRE')));
//output(DG_XMLFile_pl0(KEYED(DG_firstname='CLAIRE')));
output(DG_VarFile_pl0(KEYED(DG_firstname='CLAIRE')));

/*
// now repeat with preload(n)....

DG_FlatFile_pl2 := PRELOAD(DG_FlatFile, 2);
DG_FlatFileEvens_pl2 := PRELOAD(DG_FlatFileEvens, 2);
//DG_CSVFile_pl2 := PRELOAD(DG_CSVFile, 2);
//DG_XMLFile_pl2 := PRELOAD(DG_XMLFile, 2);
DG_VarFile_pl2 := PRELOAD(DG_VarFile, 2);

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
output(DG_VarFile_pl2(KEYED(DG_firstname='CLAIRE')));

*/

// MORE - tests: make sure that we can handle a combination of keyed/unkeyed filtering...
// MORE - preload and non-preload for the same dataset is not really testing what you think, in roxie
// MORE - in fact, unless queries get deleted, roxie will use first sight of the file even if we separate into multiple .ecl files.
